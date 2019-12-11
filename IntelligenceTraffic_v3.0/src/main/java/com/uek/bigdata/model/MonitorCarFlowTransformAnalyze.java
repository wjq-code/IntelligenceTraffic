package com.uek.bigdata.model;

import com.alibaba.fastjson.JSONObject;
import com.uek.bigdata.DAO.factory.DAOFactory;
import com.uek.bigdata.DAO.inter.ITaskDao;
import com.uek.bigdata.config.ConfigurationManager;
import com.uek.bigdata.constants.Constants;
import com.uek.bigdata.daomain.Task;
import com.uek.bigdata.mockData.MockData;
import com.uek.bigdata.utils.DateUtils;
import com.uek.bigdata.utils.ParamsUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.internal.Trees;

import java.util.*;

/**
 * 类名称: MonitorCarFlowTransformAnalyze
 * 类描述: 功能模块五 ： 卡口车流量转化率分析
 *
 * @author 武建谦
 * @Time 2018/12/3 10:52
 */
public class MonitorCarFlowTransformAnalyze {
    public static void main(String[] args){
//构建spark上下文对象：
        SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME_5);
        //判断是否是本地运行模式：因为本地运行模式才需要使用模拟数据：
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            conf.setMaster("local[*]");

        }
        //构建sparkSession对象：
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        //设置日志输出级别：
        jsc.setLogLevel("ERROR");
        if (local) {
            // 生成模拟数据
            MockData.mockData(sparkSession, jsc);
        }
        //获取任务id卡口车流量转换率模块
        long taskId = ParamsUtils.getTaskIdByArgs(null,Constants.SPARK_LOCAL_TASKID_MONITORCARFLOWTRANSFORM);
        //封装task对象
        ITaskDao taskDao = DAOFactory.getTaskDao();
        Task task = taskDao.selectTaskById(taskId);
        String taskParams = task.getTaskParams();
        JSONObject jsonObject = JSONObject.parseObject(taskParams);
        // TODO: 功能1：从数据库task表中查找出来指定的标准卡口流
        String monitorFlow = ParamsUtils.getParamByJsonStr(jsonObject, Constants.PARAM_MONITOR_FLOW);
        //对卡口流进行广播
        Broadcast<String> broadcast = jsc.broadcast(monitorFlow);
        // TODO 功能2：获取指定日期范围内的卡口详细信息：
        JavaRDD<Row> monitorInfoRDD = MonitorCarFlowAnalyze.getMonitorInfoByDateRange(sparkSession, jsonObject);
        // 把数据封装成 k v 格式 <carNum , Row>
        JavaPairRDD<String, Row> carNumByRow = getCarNumByRow(monitorInfoRDD);
        carNumByRow.persist(StorageLevel.MEMORY_AND_DISK());
        // TODO: 功能3 ： 获取到每辆车的行车轨迹中，匹配五种卡口切片的次数
        generateAndMatchRowSplit(carNumByRow,taskParams,broadcast);
    }

    /**
     * 获取到每辆车的行车轨迹中，匹配五种卡口切片的次数
     * @param carNumByRow <carNum,Row>
     * @param taskParams
     * @param monitorInfoRDD
     */
    private static void generateAndMatchRowSplit(JavaPairRDD<String, Row> carNumByRow, String taskParams, Broadcast<String> monitorInfoRDD) {
        //按车牌号分组，获取到一辆车所经过的所有卡口信息
        JavaPairRDD<String, Iterable<Row>> carNumRowByGroup = carNumByRow.groupByKey();
        carNumRowByGroup.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Long>() {
            @Override
            public Iterator<Tuple2<String, Long>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                String carNum = tuple2._1;
                List<Tuple2<String,Long>> list = new ArrayList<>();
                //
                List<Row> rows = new ArrayList<>();

                Iterator<Row> iter = tuple2._2.iterator();
                while (iter.hasNext()) {
                    Row row = iter.next();
                    rows.add(row);
                }
                //对rows集合 按照车辆通过卡扣的时间先后顺序排序:
                Collections.sort(rows, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String time1 = o1.getString(4);
                        String time2 = o2.getString(4);
                        if (DateUtils.after(time1,time2)) {
                            return 1;
                        }
                        return -1;
                    }
                });
                //对车辆行车轨迹进行拼接字符串
                StringBuilder sb = new StringBuilder();
                for (Row row : rows) {
                    String monitorId = row.getString(1);
                    sb.append(",").append(monitorId);
                }


                return null;
            }
        });
    }
    //把数据封装成 k v 格式 <carNum , Row>
    private static JavaPairRDD<String, Row> getCarNumByRow(JavaRDD<Row> monitorInfoRDD) {
        return monitorInfoRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String carNum = row.getString(3);
                return new Tuple2<>(carNum,row);
            }
        });
    }
}
