package com.uek.bigdata.model;

import com.alibaba.fastjson.JSONObject;
import com.uek.bigdata.DAO.factory.DAOFactory;
import com.uek.bigdata.DAO.inter.IRandomExtractDAO;
import com.uek.bigdata.DAO.inter.ITaskDao;
import com.uek.bigdata.config.ConfigurationManager;
import com.uek.bigdata.constants.Constants;
import com.uek.bigdata.daomain.CarTrack;
import com.uek.bigdata.daomain.RandomExtractCar;
import com.uek.bigdata.daomain.RandomExtractMonitorDetail;
import com.uek.bigdata.daomain.Task;
import com.uek.bigdata.mockData.MockData;
import com.uek.bigdata.utils.DateUtils;
import com.uek.bigdata.utils.JDBCUtils;
import com.uek.bigdata.utils.ParamsUtils;
import com.uek.bigdata.utils.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

/**
 * 类名称: RandomExtractCarInfos
 * 类描述: 随机抽取n个车辆信息统计分析
 *
 * @author 武建谦
 * @Time 2018/11/28 13:06
 */
public class RandomExtractCarInfos {
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
        Long taskId = ParamsUtils.getTaskIdByArgs(null, Constants.SPARK_LOCAL_TASKID_EXTRACTCAR);

        //封装taskId
        ITaskDao taskDao = DAOFactory.getTaskDao();
        Task task = taskDao.selectTaskById(taskId);
        String taskParams = task.getTaskParams();
        //获取到json参数
        JSONObject jsonObject = JSONObject.parseObject(taskParams);

        // TODO: 功能点一：获取抽出来的车辆信息信息
        //2.1获取到指定时间范围内的详细数据
        JavaRDD<Row> monitorInfoRDD = MonitorCarFlowAnalyze.getMonitorInfoByDateRange(sparkSession, jsonObject);
        //2.2获取到抽取出来的N辆车的详细信息
        JavaPairRDD<String, Row> randomExtractCar2DetailRDD = randomExtractCarInfos(jsc, jsonObject, taskId, monitorInfoRDD);
        randomExtractCar2DetailRDD = randomExtractCar2DetailRDD.cache();
        randomExtractCar2DetailRDD.foreach(new VoidFunction<Tuple2<String, Row>>() {
            @Override
            public void call(Tuple2<String, Row> tuple2) throws Exception {

            }
        });
//        功能点2.3、随机抽取车辆的行车轨迹分析：

    }


    /**
     * 获取抽取出N辆车的详细信息
     *
     * @param jsc            sparkContext对象
     * @param jsonObject     参数
     * @param taskId         任务id
     * @param monitorInfoRDD 指定时间范围内的数据
     */
    public static JavaPairRDD<String, Row> randomExtractCarInfos(JavaSparkContext jsc, JSONObject jsonObject, Long taskId, JavaRDD<Row> monitorInfoRDD) {
        //对一个时间段内的数据进行整理，获取到每辆车通过卡口的详细时间
        JavaPairRDD<String, String> dateHourCar2DetailRDD = monitorInfoRDD.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String actionTime = row.getString(4);
                //对日期格式进行处理
                String dateHour = DateUtils.getDateHour(actionTime);
                String carNum = row.getString(3);
                //封装信息
                String key = dateHour;
                String value = carNum;
                return new Tuple2<>(key, value);
            }
        });
        /**
         * 对相同的数据进行去重操作，因为是模拟数据，会有重复的车牌号
         */
        JavaPairRDD<String, String> dateHour2DetailRDD = dateHourCar2DetailRDD.distinct();

        //统计出每个时间段车流量的数量，有24调数据
        Map<String, Long> countByKey = dateHour2DetailRDD.countByKey();

        //封装到每一天中 存储结果数据：<日期 ， <小时 ，每小时的车流量 >>：（每天一条数据）
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<>();
        for (Map.Entry<String, Long> entry : countByKey.entrySet()) {
            //获取到每个时间段
            String dateHour = entry.getKey();
            //获取到日期
            String[] dateHoutSplit = dateHour.split("_");
            String date = dateHoutSplit[0];
            String hour = dateHoutSplit[1];
            //获取每个时间段的车流量信息
            Long count = entry.getValue();
            //每天的车流量信息，每天有24条数据
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dateHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour, count);
        }
        //获取用户指定随机抽取的总车辆数
        Integer extractNums = Integer.valueOf(ParamsUtils.getParamByJsonStr(jsonObject, Constants.FIELD_EXTRACT_NUM));
        //获取每天要随机抽取的车辆数
        int extractNumPerDay = extractNums / dateHourCountMap.size();

        /**
         *
         */
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>(64);
        //7）获取一天内总的车流量
        for (Map.Entry<String, Map<String, Long>> entry : dateHourCountMap.entrySet()) {
            String date = entry.getKey(); //日期
            Map<String, Long> hourCountMap = entry.getValue();
            Long dateCarCount = 0L; //一天内的总车流量
            for (Long count : hourCountMap.values()) {
                dateCarCount += count;
            }
            //获取每个小时车流量占总流量的百分比
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();  //每个小时
                long carCount = hourCountEntry.getValue(); //每个小时的车流量
                //当前小时需要抽取到数量
                // 当前小时内车流量  /  一天的车流量  *  抽取的总数
                int hourExtractNum = (int) (((double) carCount / (double) dateCarCount) * extractNumPerDay);
                /**
                 * 如果随机抽取车辆的数大于真实车辆数
                 */
                if (hourExtractNum > carCount) {
                    hourExtractNum = (int) carCount;
                }

                //9、<日期 ， <小时， 抽取车辆对应生成的索引>>：
                //每小时要抽取的车辆对应的索引集合；
                Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);

                if (hourExtractMap == null) {
                    hourExtractMap = new HashMap<String, List<Integer>>();
                    dateHourExtractMap.put(date, hourExtractMap);
                }
                List<Integer> extractIndexs = hourExtractMap.get(hour);
                if (extractIndexs == null) {
                    extractIndexs = new ArrayList<>();
                    hourExtractMap.put(hour, extractIndexs);
                }
                Random random = new Random();
                for (int i = 0; i < hourExtractNum; i++) {
                    //范围0-每个小时的车流量
                    int index = random.nextInt((int) carCount);
                    while (extractIndexs.contains(index)) {
                        index = random.nextInt((int) carCount);
                    }
                    extractIndexs.add(index);
                }
            }
        }
        //10)广播变量
        Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractBroadcast = jsc.broadcast(dateHourExtractMap);
        //11）从实际数据中，获取每小时内的所有车牌号：
        JavaPairRDD<String, Iterable<String>> getCarNumsByHour = dateHour2DetailRDD.groupByKey();

        //12）获取随机抽取的车辆信息（日期，小时，车牌号），并且插入到数据库：
        JavaPairRDD<String, String> carNumkvRDD = getCarNumsByHour.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                //定义用来存储随机抽取的车对象
                List<RandomExtractCar> carRandomExtracts = new ArrayList<>();
                //为下个需求做准备
                List<Tuple2<String, String>> list = new ArrayList<>();

                String dateHour = tuple2._1;
                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];

                Iterator<String> iterator = tuple2._2.iterator();

                //获取广播变量的值
                Map<String, Map<String, List<Integer>>> dateHourExtractMap = dateHourExtractBroadcast.value();
                //获取到每天的时间和每个时间下的索引
                Map<String, List<Integer>> hourAndIndexs = dateHourExtractMap.get(date);
                //获取当前小时下抽取车辆的索引
                List<Integer> carIndexs = hourAndIndexs.get(hour);

                //通过车牌索引来进行比较
                int index = 0;
                while (iterator.hasNext()) {
                    String carNum = iterator.next();
                    //如果抽取车辆的索引包含实际全量数据中车辆的索引，那么该车辆就是随机抽取的车辆：
                    if (carIndexs.contains(index)) {
                        RandomExtractCar carRandomExtract = new RandomExtractCar(taskId, carNum, date, dateHour);
                        carRandomExtracts.add(carRandomExtract);
                        //为下个需求做准备
                        list.add(new Tuple2<>(carNum, carNum));
                    }
                    index++;
                }
                //插入数据库
                IRandomExtractDAO randomExtractDAO = DAOFactory.getRandomExtractDAO();
                randomExtractDAO.insertBatchRandomExtractCar(carRandomExtracts);
                return list.iterator();
            }
        });
        //13）从实际数据中，获取每辆车的详细信息：
        JavaPairRDD<String, Row> car2DetailRDD = monitorInfoRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String carNum = row.getString(3);
                return new Tuple2<>(carNum, row);
            }
        });

        /*
         * 14）获取到抽取出来的N辆车的详细信息：
         * carNumkvRDD  <car , car>
         * car2DetailRDD  <car , row>
         * 合并后：randomExtractCar2DetailRDD    :    <car , <car , row>>
         */
        JavaPairRDD<String, Tuple2<String, Row>> joinKvRDD = carNumkvRDD.join(car2DetailRDD);

        JavaPairRDD<String, Row> randomExtractCar2DetailRDD = joinKvRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>, String, Row>() {
            @Override
            public Iterator<Tuple2<String, Row>> call(Iterator<Tuple2<String, Tuple2<String, Row>>> tuple2) throws Exception {

                List<RandomExtractMonitorDetail> randomExtractMonitorDetails = new ArrayList<>();
                List<Tuple2<String, Row>> list = new ArrayList<>();
                while (tuple2.hasNext()) {
                    Tuple2<String, Tuple2<String, Row>> tuple = tuple2.next();
                    String carNum = tuple._1;
                    Row row = tuple._2._2;
                    randomExtractMonitorDetails.add(
                            new RandomExtractMonitorDetail(
                                    taskId,
                                    row.getString(0),
                                    row.getString(1),
                                    row.getString(2),
                                    row.getString(3),
                                    row.getString(4),
                                    row.getString(5),
                                    row.getString(6)
                            )
                    );

                    list.add(new Tuple2<>(carNum, row));
                }
                //插入数据库
                IRandomExtractDAO randomExtractDAO = DAOFactory.getRandomExtractDAO();
                randomExtractDAO.insertBatchRandomExtractDetails(randomExtractMonitorDetails);
                return list.iterator();
            }
        });
        return randomExtractCar2DetailRDD;
    }

    /**
     * 功能点2.3、随机抽取车辆的行车轨迹分析：
     *
     * @param taskId
     * @param randomExtractCar2DetailRDD
     * @return
     */
    public static JavaPairRDD<String, String> getCarTrackByExtractCarInfos(Long taskId, JavaPairRDD<String, Row> randomExtractCar2DetailRDD) {
        JavaPairRDD<String, Iterable<Row>> groupByCar = randomExtractCar2DetailRDD.groupByKey();

        JavaPairRDD<String, String> carTrackRDD = groupByCar.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {

                String carNum = tuple2._1;

                Iterator<Row> iter = tuple2._2.iterator();
                //存储一辆车的所有信息
                List<Row> carMetailsIterator = new ArrayList<>();
                while (iter.hasNext()) {
                    Row row = iter.next();
                    carMetailsIterator.add(row);
                }

                //对车辆的信息通过卡口进行排序
                Collections.sort(carMetailsIterator, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String actionTime1 = o1.getString(4);
                        String actionTime2 = o2.getString(4);
                        if (DateUtils.after(actionTime1, actionTime2)) {
                            return -1;
                        }
                        return 1;
                    }
                });

                //车的轨迹
                StringBuilder carTrack = new StringBuilder();
                String monitorId = null;
                String date = null;
                for (Row row : carMetailsIterator) {
                    //获取卡口号
                    monitorId = row.getString(1);
                    date = row.getString(0);
                    //将卡口号进行拼接
                    carTrack.append("," + monitorId);
                }
                return new Tuple2<>(carNum, Constants.FIELD_DATE + "=" + date + "|" + Constants.FIELD_CAR_TRACK + "=" + carTrack.substring(1));
            }
        });
        return carTrackRDD;
    }

    public static void saveCarTrack2DB(Long taskId, JavaPairRDD<String, String> carTrackByExtractCarInfos) {
        carTrackByExtractCarInfos.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2) throws Exception {
                List<CarTrack> carTracks = new ArrayList<>();
                while (tuple2.hasNext()) {
                    Tuple2<String, String> tuple = tuple2.next();
                    String carNum = tuple._1;
                    String dateAndCarTrack = tuple._2;
                    //获取时间
                    String date = StringUtils.extractValue(dateAndCarTrack, "|", Constants.FIELD_DATE);
                    String carTrack = StringUtils.extractValue(dateAndCarTrack, "|", Constants.FIELD_CAR_TRACK);
                    carTracks.add(
                            new CarTrack(
                                    taskId,
                                    carNum,
                                    date,
                                    carTrack
                            )
                    );
                    //插入数据库
                    IRandomExtractDAO randomExtractDAO = DAOFactory.getRandomExtractDAO();
                    randomExtractDAO.insertBatchCarTrack(carTracks);
                }
            }
        });
    }
}
