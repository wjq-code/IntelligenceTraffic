package com.uek.bigdata.model;

import com.alibaba.fastjson.JSONObject;
import com.uek.bigdata.DAO.factory.DAOFactory;
import com.uek.bigdata.DAO.inter.IMonitorDAO;
import com.uek.bigdata.DAO.inter.ITaskDao;
import com.uek.bigdata.model.accumulator.MonitorAndCameraStateAccumulator;
import com.uek.bigdata.config.ConfigurationManager;
import com.uek.bigdata.constants.Constants;
import com.uek.bigdata.daomain.*;
import com.uek.bigdata.mockData.MockData;
import com.uek.bigdata.utils.ParamsUtils;
import com.uek.bigdata.utils.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

/**
 * 类名称：MonitorCarFlowAnalyze
 * 类描述：功能模块1 卡口车流量分析统计
 *
 * @author 武建谦
 * @Time 2018/11/25 15:21
 */
public class MonitorCarFlowAnalyze {
    public static void main(String[] args){
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
        MockData.mockData(sparkSession,jsc);

        //TODO 完成功能1.1：卡口车流量监控模块
        //获取任务id  卡口车流量监控模块
        long taskId = ParamsUtils.getTaskIdByArgs(null,Constants.SPARK_LOCAL_TASKID_MONITORFLOW);

        //封装task对象
        ITaskDao taskDao = DAOFactory.getTaskDao();
        //根据task的主键查询指定的任务数据
        Task task = taskDao.selectTaskById(taskId);
        //将json字符串装换为json对象
        JSONObject taskParams = JSONObject.parseObject(task.getTaskParams());

        /**
         * 获取到平台使用者指定日期范围内的卡口详细信息
         */
        //1.1获取指定日期范围内的卡口数据（原始数据格式）
        JavaRDD<Row> monitorInfoRDD = getMonitorInfoByDateRange(sparkSession, taskParams);
        //缓冲操作
        monitorInfoRDD = monitorInfoRDD.cache();

        /**
         * 1.2对指定日期范围内的卡口数据进行整合，获取到kv格式的数据，key是卡口id，value是卡口对象的数据
         */
        JavaPairRDD<String, Row> monitorInfoKVRDD = getMonitorInfoKVRDDByMonitorInfoRDD(monitorInfoRDD);
        //缓存
        monitorInfoKVRDD = monitorInfoKVRDD.cache();

        /**
         * 1.3按照卡口id进行分组统计：
         */
        JavaPairRDD<String, Iterable<Row>> monitorInfoKVRDDByGroup = monitorInfoKVRDD.groupByKey();
        //缓存
        monitorInfoKVRDDByGroup = monitorInfoKVRDDByGroup.cache();

        /**
         * 1.4对按照卡口id分组统计后的数据格式，进行整理：获取到的kv格式的RDD的key是卡口id , value是该卡口id对应的数据拼接的字符串；
         */
        JavaPairRDD<String, String> monitorInfoKVRDDByTrim = getMonitorInfoKVRDDByTrim(monitorInfoKVRDDByGroup);
        monitorInfoKVRDDByTrim = monitorInfoKVRDDByTrim.cache();
        //卡口车流量信息
        System.out.println("卡口车流量信息");
        monitorInfoKVRDDByTrim.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
        //TODO 完成功能1.2：检测各卡口的运行状态是否正常
        //自定义5中累加器
        Accumulator<String> accumulator = jsc.accumulator("", new MonitorAndCameraStateAccumulator());

        //检测各卡口的运行状态，并使用累加器把各种状态添加到累加器中
        JavaPairRDD<Integer, String> getCarCountByMonitorInfos = checkMonitorState(sparkSession, jsc, accumulator, taskId, taskParams, monitorInfoKVRDDByTrim);
        getCarCountByMonitorInfos = getCarCountByMonitorInfos.cache();
        getCarCountByMonitorInfos.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
        //TODO 完成功能1.3：把自定义累加器记录的五种状态插入到mysql数据库：
        insertAccumulatorResultToMySQL(taskId,accumulator);

        //TODO 获取车流排名前N的卡口号，并把结果插入到数据库
        JavaPairRDD<String, String> topNMonitor2CarFlow = getMonitorCarFlowTopN(jsc, taskId, taskParams, getCarCountByMonitorInfos);
        topNMonitor2CarFlow = topNMonitor2CarFlow.cache();

        // TODO 获取车流量最大的topN卡口的详细信息，并且持久化到数据库中
        getTopNDetails(taskId,topNMonitor2CarFlow,monitorInfoKVRDD);

        //TODO 完成功能1.4：获取经常被高速通过的Top5卡口id
        List<String> top5MonitorIds = speedTopNMonitor(monitorInfoKVRDDByGroup);
        for (String top5MonitorId : top5MonitorIds) {
            System.out.println("高速通过的5个卡口有:  " + top5MonitorId);
        }
        // TODO 获取被高速通过的前5个卡口中，每个卡口下的最快的10辆车的信息：
        getMonitorDetails(jsc,taskId,top5MonitorIds,monitorInfoKVRDD);

        //释放资源
        jsc.close();
        sc.stop();
        sparkSession.close();

    }

    /**
     * 获取被高速通过的前5个卡口中，每个卡口下的最快的10辆车的信息：
     * @param jsc
     * @param taskId
     * @param top5MonitorIds
     * @param monitorInfoKVRDD
     */
    private static void getMonitorDetails(JavaSparkContext jsc, long taskId, List<String> top5MonitorIds, JavaPairRDD<String, Row> monitorInfoKVRDD) {
        
        //将被高速通过的前5个卡口的卡口号进行广播：
        Broadcast<List<String>> top5MonitorIdsBroadcast = jsc.broadcast(top5MonitorIds);

        //从全量数据中过滤出来前5个卡口的信息
        JavaPairRDD<String, Row> filterRDD = monitorInfoKVRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
                String monitorIds = tuple2._1;  //卡口id
                List<String> list = top5MonitorIdsBroadcast.value();
                return list.contains(monitorIds);
            }
        });

        //对前5个卡口的信息进行汇总
        JavaPairRDD<String, Iterable<Row>> groupByKeyRDD = filterRDD.groupByKey();

        //存放每个卡口下的10辆车的信息
        List<Row> list = new ArrayList<>();
        groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Row>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                String monitorId = tuple2._1;
                Iterator<Row> iter = tuple2._2.iterator();
                while (iter.hasNext()) {
                    Row row = iter.next();
                    if (row.getString(1).equals(monitorId)) {
                        list.add(row);
                    }
                }
                Collections.sort(list, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        Integer speed1 = Integer.valueOf(o1.getString(5));
                        Integer speed2 = Integer.valueOf(o2.getString(5));
                        return Integer.compare(speed2,speed1);
                    }
                });
                //获取到前10个车速最快的
                List<Row> top10List = new ArrayList<>();
                if (list.size() >= 10) {
                    top10List = list.subList(0, 10);
                }
                for (Row row : top10List) {
                    System.out.println(monitorId+"分区下：车速最快的是："+row);
                }
                System.out.println(monitorId+"____"+top10List.size());
                List<TopNMonitorDetailInfo> topNMonitorDetailInfos = new ArrayList<>();
                for (Row row : top10List) {
                    topNMonitorDetailInfos.add(
                            new TopNMonitorDetailInfo(
                                    taskId+"",
                                    row.getString(0),
                                    row.getString(1),
                                    row.getString(2),
                                    row.getString(3),
                                    row.getString(4),
                                    row.getString(5),
                                    row.getString(6)
                            )
                    );
                }
                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                monitorDAO.insertBatchTop10Details(topNMonitorDetailInfos);
                list.clear();
            }
        });


    }
    

    /**
     * 获取经常被高速通过的Top5卡口id
     * @param monitorInfoKVRDDByGroup
     */
    private static List<String> speedTopNMonitor(JavaPairRDD<String, Iterable<Row>> monitorInfoKVRDDByGroup) {
        JavaPairRDD<SpeedSortKey, String> groupByMonitorId = monitorInfoKVRDDByGroup.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, SpeedSortKey, String>() {
            @Override
            public Tuple2<SpeedSortKey, String> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {

                String monitorId = tuple2._1;
                Iterator<Row> iter = tuple2._2.iterator();
                long lowSpeed = 0;
                long normalSpeed = 0;
                long mediumSpeed = 0;
                long highSpeed = 0;

                while (iter.hasNext()) {
                    Row row = iter.next();
                    Integer speed = Integer.valueOf(row.getString(5));

                    if (speed >= 0 && speed < 60) {
                        //低速
                        lowSpeed++;
                    } else if (speed >= 60 && speed <= 90) {
                        //正常
                        normalSpeed++;
                    } else if (speed >= 90 && speed < 120) {
                        //中速
                        mediumSpeed++;
                    } else if (speed >= 120) {
                        //高速
                        highSpeed++;
                    }
                }
                SpeedSortKey speedSortKey = new SpeedSortKey(lowSpeed, normalSpeed, mediumSpeed, highSpeed);
                return new Tuple2<>(speedSortKey, monitorId);
            }
        });

        JavaPairRDD<SpeedSortKey, String> sortBySpeedCount = groupByMonitorId.sortByKey(false,1);
        sortBySpeedCount.foreach(new VoidFunction<Tuple2<SpeedSortKey, String>>() {
            @Override
            public void call(Tuple2<SpeedSortKey, String> tuple2) throws Exception {
                System.out.println("卡口有："+tuple2);
            }
        });
        List<Tuple2<SpeedSortKey, String>> take = sortBySpeedCount.take(5);

        List<String> monitorIds = new ArrayList<>();
        for (Tuple2<SpeedSortKey, String> tuple2 : take) {
            String monitorId = tuple2._2;
            monitorIds.add(monitorId);
        }
        return monitorIds;
    }

    /**
     *  获取车流量最大的topN卡口的详细信息，并且持久化到数据库中
     * @param taskId
     * @param topNMonitor2CarFlow
     * @param monitorInfoKVRDD
     */
    private static void getTopNDetails(long taskId, JavaPairRDD<String, String> topNMonitor2CarFlow, JavaPairRDD<String, Row> monitorInfoKVRDD) {
        JavaPairRDD<String, Tuple2<String, Row>> joinRDD = topNMonitor2CarFlow.join(monitorInfoKVRDD);

        joinRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
                String monitorId = tuple2._1;
                Row row = tuple2._2._2;
                return new Tuple2<>(monitorId,row);
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Row>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Row>> iter) throws Exception {
                List<TopNMonitorDetailInfo> monitorDetailInfos  = new ArrayList<>();
                while (iter.hasNext()) {
                    Tuple2<String, Row> tuple = iter.next();
                    Row row = tuple._2;
                    monitorDetailInfos.add(new TopNMonitorDetailInfo(
                            taskId+"",
                            row.getString(0),
                            row.getString(1),
                            row.getString(2),
                            row.getString(3),
                            row.getString(4),
                            row.getString(5),
                            row.getString(6)
                    ));
                }
                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                monitorDAO.insertBatchMonitorDetails(monitorDetailInfos);
            }
        });



    }

    public static JavaPairRDD<String, String> getMonitorCarFlowTopN(JavaSparkContext jsc, long taskId, JSONObject taskParams, JavaPairRDD<Integer, String> getCarCountByMonitorInfos) {
        
        //1)获取客户端要求的topN的数量
        Integer getTopNumByParams = Integer.valueOf(ParamsUtils.getParamByJsonStr(taskParams, Constants.FIELD_TOP_NUM));
        //2）通过降序排序，获取用户指定的topN信息：
        List<Tuple2<Integer, String>> topNCarCount = getCarCountByMonitorInfos.sortByKey(false).take(getTopNumByParams);
        //存储数据，批量执行
        List<TopNMonitor2CarCount> topNMonitor2CarCounts = new ArrayList<>();
        for (Tuple2<Integer, String> tuple : topNCarCount) {
            Integer carCount = tuple._1;
            String monitorId = tuple._2;
            topNMonitor2CarCounts.add(new TopNMonitor2CarCount(taskId,monitorId,carCount));
        }
        IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
        monitorDAO.insertBatchTopN(topNMonitor2CarCounts);


        /**
         * 转换数据为下面功能实现做准备
         */
        List<Tuple2<String,String>> monitorId2CarCounts = new ArrayList<Tuple2<String,String>>();
        for (Tuple2<Integer, String> tuple2 : topNCarCount) {
            monitorId2CarCounts.add(new Tuple2<String, String>(tuple2._2,tuple2._2));
        }
        JavaPairRDD<String, String> t1T2JavaPairRDD = jsc.parallelizePairs(monitorId2CarCounts);
    }

    public static void insertAccumulatorResultToMySQL(long taskId, Accumulator<String> accumulator) {
        //获取累加器的值
        String accumulatorVal = accumulator.value();
        String normalMonitorCount = StringUtils.extractValue(accumulatorVal,"|",Constants.FIELD_NORMAL_MONITOR_COUNT);
        String normalCameraCount = StringUtils.extractValue(accumulatorVal,"|",Constants.FIELD_NORMAL_CAMERA_COUNT);
        String abnormalMonitorCount = StringUtils.extractValue(accumulatorVal,"|",Constants.FIELD_ABNORMAL_MONITOR_COUNT);
        String abnormalCameraCount = StringUtils.extractValue(accumulatorVal,"|",Constants.FIELD_ABNORMAL_CAMERA_COUNT);
        String abnormalMonitorCameraInfos = StringUtils.extractValue(accumulatorVal,"|",Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS);
        MonitorState monitorState = new MonitorState(taskId, normalMonitorCount, normalCameraCount, abnormalMonitorCount, abnormalCameraCount, abnormalMonitorCameraInfos);
        IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
        monitorDAO.insertMonitorState(monitorState);
    }

    //1.1获取指定日期范围内的卡口数据（原始数据格式）
    public static JavaRDD<Row> getMonitorInfoByDateRange(SparkSession sparkSession , JSONObject taskParams){
        //获取到用户输入的起始时间：
        String startDate = ParamsUtils.getParamByJsonStr(taskParams, Constants.PARAM_START_DATE);
        //获取到用户输入的结束时间：
        String endtDate = ParamsUtils.getParamByJsonStr(taskParams, Constants.PARAM_END_DATE);

        //sql语句
        String sql  = "select * from monitor_flow_action where date >='" + startDate+"'and date <= '"+ endtDate +"'";
        Dataset<Row> rang = sparkSession.sql(sql);
        return rang.javaRDD();
    }
    /**
     * 1.2对指定日期范围内的卡口数据进行整合，获取到kv格式的数据，key是卡口id，value是卡口对象的数据
     */
    public static JavaPairRDD<String ,Row> getMonitorInfoKVRDDByMonitorInfoRDD(JavaRDD<Row> monitorInfoRDD){
        JavaPairRDD<String, Row> stringRowJavaPairRDD = monitorInfoRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String monitor_id = row.getString(1);
                return new Tuple2<>(monitor_id, row);
            }
        });
        return  stringRowJavaPairRDD;
    }

    /**
     *  对按照卡口id分组统计后的数据格式，进行整理：获取到的kv格式的RDD的key是卡口id , value是该卡口id对应的数据拼接的字符串；
     */
    public static JavaPairRDD<String , String> getMonitorInfoKVRDDByTrim(JavaPairRDD<String, Iterable<Row>> monitorInfoKVRDDByGroup){
        JavaPairRDD<String, String> stringStringJavaPairRDD = monitorInfoKVRDDByGroup.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                //卡口id
                String monitorId = tuple2._1;

                //当前卡口下进过所有车辆信息，每一个row对象代表一辆车的信息
                Iterator<Row> iter = tuple2._2.iterator();

                //把卡口下的所有摄像头添加到集合中存储
                List<String> list = new ArrayList<>();
                //用来存储拼接每个摄像头信息
                StringBuilder sb = new StringBuilder();
                //记录卡口下通过的车辆数
                int count = 0;
                //记录区域
                String areaId = null;
                while (iter.hasNext()) {
                    //每一辆车的信息
                    Row row = iter.next();
                    //区域
                    areaId = row.getString(7);
                    //摄像头id
                    String cameraId = row.getString(2);
                    if (!list.contains(cameraId)) {
                        list.add(cameraId);
                    }
                    //车辆数
                    count++;
                }
                //拼接摄像头
                for (String cameraId : list) {
                    sb.append(",").append(cameraId);
                }
                int cameraCount = list.size();
                String monitorIdInfo =
                        Constants.FIELD_MONITOR_ID + "=" + monitorId + "|"
                                + Constants.FIELD_AREA_ID + "=" + areaId + "|"
                                + Constants.FIELD_CAMERA_IDS + "=" + sb.toString().substring(1) + "|"
                                + Constants.FIELD_CAMERA_COUNT + "=" + cameraCount + "|"
                                + Constants.FIELD_CAR_COUNT + "=" + count;

                return new Tuple2<>(monitorId, monitorIdInfo);
            }
        });
        return stringStringJavaPairRDD;
    }

    public static JavaPairRDD<Integer , String> checkMonitorState(
            SparkSession sparkSession,
            JavaSparkContext jsc,
            Accumulator<String> accumulator,
            long taskId,
            JSONObject taskParams,
            JavaPairRDD<String , String> monitorInfoKVRDDByTrim){

        //获取数据字典中的标准数据
        String sql = "select * from monitor_camera_info";
        Dataset<Row> dataSet = sparkSession.sql(sql);

        //转化为javaRDD
        JavaRDD<Row> dataDictionariesRowRDD = dataSet.javaRDD();
        JavaPairRDD<String, String> getMonitorIdAndCameraIdByRow = dataDictionariesRowRDD.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String monitorId = row.getString(0);
                String cameraId = row.getString(1);
                return new Tuple2<>(monitorId, cameraId);
            }
        });
        //持久化
        getMonitorIdAndCameraIdByRow = getMonitorIdAndCameraIdByRow.cache();
        JavaPairRDD<String, Iterable<String>> getCameraIdByMonitorIdGroup = getMonitorIdAndCameraIdByRow.groupByKey();
        //持久化
        getCameraIdByMonitorIdGroup = getCameraIdByMonitorIdGroup.cache();

        JavaPairRDD<String, String> getmonitorIdAndCameraInfosByTrim = getCameraIdByMonitorIdGroup.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                String monitorId = tuple2._1;
                Iterator<String> iter = tuple2._2.iterator();
                StringBuilder sb = new StringBuilder();
                //数据字典中一个卡口下的摄像头计数器
                int count = 0;
                while (iter.hasNext()) {
                    sb.append(","+iter.next());
                    count++;
                }
                String cameraInfo =
                        Constants.FIELD_CAMERA_IDS + "=" + sb.toString().substring(1) + "|"
                        + Constants.FIELD_CAMERA_COUNT + "=" +count;
                return new Tuple2<>(monitorId, cameraInfo);
            }
        });
        getmonitorIdAndCameraInfosByTrim = getmonitorIdAndCameraInfosByTrim.cache();

        //数据字典表中的数据和卡口流量监控进行join操作
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinPairRDD = getmonitorIdAndCameraInfosByTrim.leftOuterJoin(monitorInfoKVRDDByTrim);

        return  joinPairRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<String, Optional<String>>>>, Integer, String>() {
            @Override
            public Iterator<Tuple2<Integer, String>> call(Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> iter) throws Exception {
                //存储最终结果的数据
                List<Tuple2<Integer,String>> list = new ArrayList<>();
                //遍历每个分区中的数据
                while (iter.hasNext()) {
                    Tuple2<String, Tuple2<String, Optional<String>>> tuple2 = iter.next();
                    String monitorId = tuple2._1;                     															//卡口id
                    String standardCameraInfos = tuple2._2._1;        											//卡口id对应的摄像头的标准数据
                    Optional<String> practicalCameraInfos = tuple2._2._2;       					//卡口id对应的摄像头的实际数据
                    String practicalCameraInfosStr = "";        //实际情况卡口下摄像头信息

                    if (!practicalCameraInfos.isPresent()) {
                        //代表卡口下的摄像头都坏了
                        String standardCameraIds = StringUtils.extractValue(standardCameraInfos, "|", Constants.FIELD_CAMERA_IDS);
                        String[] cameraIds = standardCameraIds.split(",");
                        //异常摄像头的数量
                        int abnoramlCameraCount = cameraIds.length;
                        //异常摄像头的拼接
                        StringBuffer abnormalCameraInfos = new StringBuffer();
                        abnormalCameraInfos.append(monitorId).append(":");
                        for (String cameraId : cameraIds) {
                            abnormalCameraInfos.append("," + cameraId);
                        }

                        //更新累加器的记录状态
                        accumulator.add(
                                Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|"
                                + Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnoramlCameraCount+"|"
                                + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + abnormalCameraInfos.toString().substring(1)
                        );
                        //应为整个卡口都坏掉了，不需要下面的操作
                        continue;
                    }else {
                        //如果卡口下有实际数据，说明该卡口正常（但有摄像头坏掉的情况）
                        practicalCameraInfosStr = practicalCameraInfos.get();
                    }
                    //获取实际摄像头中的数量
                    String practicalCameraCountStr = StringUtils.extractValue(practicalCameraInfosStr, "|", Constants.FIELD_CAMERA_COUNT);
                    int practicalCameraCount = Integer.valueOf(practicalCameraCountStr);
                    
                    //获取数据字典中的摄像头数量
                    String standardCameraCountStr = StringUtils.extractValue(standardCameraInfos, "|", Constants.FIELD_CAMERA_COUNT);
                    int standardCameraCount = Integer.valueOf(standardCameraCountStr);

                    if (practicalCameraCount == standardCameraCount) {
                        accumulator.add(
                                Constants.FIELD_NORMAL_MONITOR_COUNT + "=1|"
                                + Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + standardCameraCount
                        );
                    }else{
                        //获取实际摄像头中的数量
                        String practicalCameraIds = StringUtils.extractValue(practicalCameraInfosStr, "|", Constants.FIELD_CAMERA_IDS);
                        List<String> practicalCameraIdList = Arrays.asList(practicalCameraIds.split(","));
                        //获取数据字典中的摄像头数量
                        String standardCameraIds = StringUtils.extractValue(standardCameraInfos, "|", Constants.FIELD_CAMERA_IDS);
                        List<String> standardCameraIdList = Arrays.asList(standardCameraIds.split(","));

                        StringBuilder abnormalCameraInfos = new StringBuilder();
                        int abnormalCmeraCount = 0;
                        int normalCameraCount = 0;

                        for (String standardCameraId : standardCameraIdList) {
                            if (!practicalCameraIdList.contains(standardCameraId)) {
                                abnormalCmeraCount ++;
                                abnormalCameraInfos.append(","+standardCameraId);
                            }
                            normalCameraCount ++;
                        }
                        //更新自定义累加器
                        accumulator.add(
                                Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + normalCameraCount + "|"
                                + Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|"
                                + Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnormalCmeraCount + "|"
                                + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + abnormalCameraInfos.substring(1)
                        );

                    }
                    //从实际数据中获取到当前卡口下的车辆信息
                    String carCountStr = StringUtils.extractValue(practicalCameraInfosStr, "|", Constants.FIELD_CAR_COUNT);
                    Integer carCount = Integer.valueOf(carCountStr);
                    list.add(new Tuple2<Integer,String>(carCount,monitorId));
                }
                return list.iterator();
            }
        });
    }
}
