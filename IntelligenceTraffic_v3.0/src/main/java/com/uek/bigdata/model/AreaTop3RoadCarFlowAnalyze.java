package com.uek.bigdata.model;

import com.alibaba.fastjson.JSONObject;
import com.uek.bigdata.DAO.factory.DAOFactory;
import com.uek.bigdata.DAO.inter.ITaskDao;
import com.uek.bigdata.config.ConfigurationManager;
import com.uek.bigdata.constants.Constants;
import com.uek.bigdata.daomain.Task;
import com.uek.bigdata.mockData.MockData;
import com.uek.bigdata.model.udaf.GroupConcatDistinctUDAF;
import com.uek.bigdata.utils.ParamsUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 类名称: AreaTop3RoadCarFlowAnalyze
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/29 10:42
 */
public class AreaTop3RoadCarFlowAnalyze {
    public static void main(String[] args) {
        //构建spark上下文对象
        SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME_1);
        //判断是否是本地运行模式,因为本地运行才需要使用模拟数据
        //判断时候为本地运行模式
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if (local) {
            conf.setMaster("local[*]");
        }
        //SparkSession对象
        SparkSession sparkSession = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        jsc.setLogLevel("WARN");
        //生成模拟数据
        MockData.mockData(sparkSession, jsc);

        //获取taskid
        Long taskId = ParamsUtils.getTaskIdByArgs(null, Constants.SPARK_LOCAL_TASKID_ROADFLOWTOPN);
        System.out.println("各个区域topN的车流量：" + taskId);

        //封装task 获取参数
        ITaskDao taskDao = DAOFactory.getTaskDao();
        Task task = taskDao.selectTaskById(taskId);
        JSONObject jsonObject = JSONObject.parseObject(task.getTaskParams());

        //注册自定义函数
        sparkSession.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());

        /**
         * 功能1 ：获取用户指定时间段内的卡口详细信息，数据格式：<区域id , Row>
         */
        JavaPairRDD<String, Row> getAreaIdByRow = getInfosByDateRDD(sparkSession, jsonObject);
        getAreaIdByRow.foreach(new VoidFunction<Tuple2<String, Row>>() {
            @Override
            public void call(Tuple2<String, Row> tuple2) throws Exception {
                System.out.println("指定数据："+tuple2);
            }
        });
        /**
         * 功能2：从异构数据源MySQL中获取区域标准数据，数据格式：<area_id , row详细信息>
         */
        JavaPairRDD<String, Row> areaId2AreaInfoRDD = getAreaId2AreaInfoRDD(sparkSession);

        /**
         * 功能3：和标准数据进行join ,补全区域信息,添加区域名称 , 封装Row对象 : areaId, areaName, roadId, monitorId, carNum
         * 			  生成基础临时信息表 ： tmp_car_flow_basic
         */
        generateTempRoadFlowBasicTable(sparkSession, areaId2AreaInfoRDD, getAreaIdByRow);

        /**
         * 功能4 ： 统计各个区域内的各个路段的车流量：
         *					生成临时表： tmp_area_road_flow_count
         */
        generateTempAreaRoadFlowTable(sparkSession);

        /**
         *使用开窗函数  获取每一个区域的topN路段：
         */
        getAreaTop3RoadFolwRDD(sparkSession);
    }

    private static void getAreaTop3RoadFolwRDD(SparkSession sparkSession) {
        String sql = "select\n" +
                "    area_name,\n" +
                "    road_id,\n" +
                "    car_count,\n" +
                "    monitor_infos,\n" +
                "    CASE\n" +
                "        when car_count > 170  THEN 'A LEVEL'\n" +
                " when car_count > 160 AND car_count <= 170\n" +
                " THEN 'B LEVEL'\n" +
                "        when car_count > 150 AND car_count <= 160 THEN 'C LEVEL'\n" +
                "        ELSE 'D LEVEL'\n" +
                " END flow_Level\n" +
                " from\n" +
                "    (SELECT\n" +
                "        area_name,\n" +
                "        road_id,\n" +
                "        car_count,\n" +
                "        monitor_infos,\n" +
                "        rank() over(partition by area_name order by car_count desc) rn\n" +
                "    FROM tmp_area_road_flow_count\n" +
                ") tmp\n" +
                " where rn <= 3";
        sparkSession.sql(sql).show();
    }

    public static void generateTempAreaRoadFlowTable(SparkSession sparkSession) {
        String sql = "SELECT area_name,road_id,COUNT (*) car_count,group_concat_distinct(monitor_id) monitor_infos FROM tmp_car_flow_basic\n" +
                " GROUP  BY area_name,road_id";
        Dataset<Row> dataset = sparkSession.sql(sql);
        dataset.createOrReplaceTempView("tmp_area_road_flow_count");
        dataset.show();

    }

    /**
     * 功能3：和标准数据进行joing ,补全区域信息,添加区域名称 , 封装Row对象 : areaId, areaName, roadId, monitorId, carNum
     * monitor_id,"
     * car_num,"
     * road_id,"
     * area_id "
     * 生成基础临时信息表 ： tmp_car_flow_basic
     */
    public static void generateTempRoadFlowBasicTable(SparkSession sparkSession, JavaPairRDD<String, Row> areaId2AreaInfoRDD, JavaPairRDD<String, Row> getAreaIdByRow) {
        JavaPairRDD<String, Tuple2<Row, Row>> joinKVRDD = areaId2AreaInfoRDD.join(getAreaIdByRow);
        JavaRDD<Row> getAreaInfosByJoin = joinKVRDD.map(new Function<Tuple2<String, Tuple2<Row, Row>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Row, Row>> tuple2) throws Exception {
                String areaId = tuple2._1; //区域id
                Row areaDetailRow = tuple2._2._1;   //标准数据信息
                Row carFlowDetailRow = tuple2._2._2;   //实际数据信息

                String monitorId = carFlowDetailRow.getString(0);   //卡口id
                String carNum = carFlowDetailRow.getString(1);   //车牌号
                String roadId = carFlowDetailRow.getString(2);   //道路id

                String areaName = areaDetailRow.getString(1);

                Row row = RowFactory.create(areaId, areaName, roadId, monitorId, carNum);
                return row;
            }
        });

        StructType structType = DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("area_id", DataTypes.StringType, true),
                        DataTypes.createStructField("area_name", DataTypes.StringType, true),
                        DataTypes.createStructField("road_id", DataTypes.StringType, true),
                        DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
                        DataTypes.createStructField("car_num", DataTypes.StringType, true)
                )
        );
        Dataset<Row> dateSet = sparkSession.createDataFrame(getAreaInfosByJoin, structType);
        dateSet.createOrReplaceTempView("tmp_car_flow_basic");
        dateSet.show();
    }

    /**
     * 功能2：从异构数据源MySQL中获取区域标准数据，数据格式：<area_id , row详细信息>
     */
    public static JavaPairRDD<String, Row> getAreaId2AreaInfoRDD(SparkSession sparkSession) {
        String url = null;
        String user = null;
        String password = null;
        Boolean flag = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (flag) {
            //本地运行模式
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            //集群运行模式
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        }
        Map<String, String> option = new HashMap<String, String>();
        option.put("url", url);
        option.put("user", user);
        option.put("password", password);
        option.put("dbtable", "area_info");
        Dataset<Row> dateSet = sparkSession.read().format("jdbc").options(option).load();
        JavaRDD<Row> areaInfoRDD = dateSet.javaRDD();
        JavaPairRDD<String, Row> getAreaIdByAreaInfo = areaInfoRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row);
            }
        });
        return getAreaIdByAreaInfo;
    }

    /**
     * 功能1：获取用户指定时间段内的卡口详细信息，数据格式：<区域id , Row>
     */
    private static JavaPairRDD<String, Row> getInfosByDateRDD(SparkSession sparkSession, JSONObject jsonObject) {
        String startDate = ParamsUtils.getParamByJsonStr(jsonObject, Constants.PARAM_START_DATE);
        String endDate = ParamsUtils.getParamByJsonStr(jsonObject, Constants.PARAM_END_DATE);
        String sql = "SELECT "
                + "monitor_id,"
                + "car_num,"
                + "road_id,"
                + "area_id "
                + "FROM	 monitor_flow_action "
                + "WHERE date >= '" + startDate + "'"
                + "AND date <= '" + endDate + "'";
        Dataset<Row> dateSet = sparkSession.sql(sql);
        dateSet.show();
        JavaRDD<Row> rowRDD = dateSet.javaRDD();
        JavaPairRDD<String, Row> getAreaIdByRow = rowRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(3), row);
            }
        });
        return getAreaIdByRow;
    }

}
