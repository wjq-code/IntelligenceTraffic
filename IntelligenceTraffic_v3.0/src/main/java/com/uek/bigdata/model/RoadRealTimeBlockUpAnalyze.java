package com.uek.bigdata.model;

import com.uek.bigdata.config.ConfigurationManager;
import com.uek.bigdata.constants.Constants;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 类名称: RoadRealTimeBlockUpAnalyze
 * 类描述:  功能模块六 ： 道路实时拥堵统计
 *
 * @author 武建谦
 * @Time 2018/11/29 17:48
 */
public class RoadRealTimeBlockUpAnalyze/* implements Serializable*/ {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME_4);
        conf.setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //设置日志的输出级别
        jsc.setLogLevel("WARN");
        //判断运行模式
        Boolean flag = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (flag) {
            //本地运行
            conf.setMaster("local[*]");
        }


        //构建sparkStreaming对象
        JavaStreamingContext sparkStreaming = new JavaStreamingContext(jsc, Durations.seconds(5));
        //指定持久化目录：
        sparkStreaming.checkpoint("testData/checkpoint");
        //设置kafka相关配置
        Map<String, String> kafkaParams = new HashMap<>();
        //broker实例
        String brokerList = ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST);
        kafkaParams.put("bootstrap.servers", brokerList);
//        要消费的topic
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<>();
        for (String topic : kafkaTopicsSplited) {
            topics.add(topic);
        }

        //kafka + SparkStreaming Direct模式
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(
                sparkStreaming,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );
        //TODO:1,1道路拥挤实时统计
        roadRealTimeBlockUpAnalyze(directStream);

        /**
         * 功能2：稽查布控（数据字典文件（记录的是非法车辆，车牌号））；在实时生成的数据当中，如果有该文件中出现的非法车辆，那么要把该车的详细信息获取；
         */
        String path = "IntelligenceTraffic_v3.0/testData/test.txt";
        JavaDStream<String> controlCarDStream = controlCar(path, directStream, jsc);

        controlCarDStream.print();

        sparkStreaming.start();
        sparkStreaming.awaitTermination();

//        sparkSession.close();
        jsc.close();
    }

    /**
     * 功能二：缉查布控
     *
     * @param path
     * @param directStream
     * @param jsc
     */
    private static JavaDStream<String> controlCar(String path, JavaPairInputDStream<String, String> directStream, JavaSparkContext jsc) {

        System.out.println("稽查犯人中........");
        //读取每一行数据
        JavaDStream<String> dataDStream = directStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._2;
            }
        });
        JavaDStream<String> controlCarDStream = dataDStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                SparkContext context = rdd.context();
                JavaSparkContext jsc = new JavaSparkContext(context);
                /**
                 * 在Driver端运行，没5秒执行一次
                 */
                //定义广播变量
                /**
                 * 读取一个文件，转换为List<String>
                 */
                rdd.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        System.out.println("稽查中："+s);
                    }
                });
                List<String> blackCars = readFile(path);
                //广播变量
                Broadcast<List<String>> broadcast = jsc.broadcast(blackCars);
                JavaRDD<String> filterRDD = rdd.filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String v1) throws Exception {
                        List<String> list = broadcast.value();
                        String carNum = v1.split("\t")[3];
                        System.out.println("经过的车牌号: "+carNum);
                        return list.contains(carNum);
                    }
                });

                JavaRDD<String> mapRDD = filterRDD.map(new Function<String, String>() {
                    @Override
                    public String call(String v1) throws Exception {
                        //获取该车辆的卡口信息和时间
                        System.out.println("犯人车辆："+v1);
                        String[] split = v1.split("\t");
                        //把这些信息写到数据中
                        return v1;
                    }
                });

                return mapRDD;
            }

            /**
             * 读取指定的通缉的车辆文件
             * @param path
             * @return
             */
            private List<String> readFile(String path) {
                List<String> list = new ArrayList<>();
                System.out.println("读取嫌疑车辆中.......");
                try {
                    BufferedReader read = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
                    String line = read.readLine();
                    while ((line != null)) {
                        list.add(line);
                        line = read.readLine();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return list;
            }
        });
        return controlCarDStream;
    }

    /**
     * 功能1 ： 道路实时拥堵统计，每1分钟统计一次；
     *
     * @param directStream
     */
    private static void roadRealTimeBlockUpAnalyze(JavaPairInputDStream<String, String> directStream) {
        //1.通过DStreaming来获取每一行真实数据
        JavaDStream<String> dataDStream = directStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                /**
                 * 第一个是offset
                 * 第二个是真实的数据
                 */
                return v1._2;
            }
        });
        /**
         * 拿到车辆的信息，对车辆的信息进行处理
         */
        JavaPairDStream<String, String> getMonitorIdAndSpeedByDStream = dataDStream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] split = s.split("\t");
                String monitorId = split[1];
                String speed = split[5];
                return new Tuple2<>(monitorId, speed);
            }
        });
        /**
         * 对数据的value进行添加标记
         */
        JavaPairDStream<String, Tuple2<Integer, Integer>> labelSpeedDStream = getMonitorIdAndSpeedByDStream.mapValues(new Function<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(String v1) throws Exception {
                return new Tuple2<>(Integer.valueOf(v1), 1);
            }
        });

        /**
         * 使用开窗函数，保存每个卡口下的车辆速度总和
         * 窗口的长度是一分钟，处理时间10秒
         */
        JavaPairDStream<String, Tuple2<Integer, Integer>> resultDStream = labelSpeedDStream.reduceByKeyAndWindow(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
            }
        }, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1 - v2._1, v1._2 - v2._2);
            }
        }, Durations.minutes(1), Durations.seconds(10));
        //遍历结果
        resultDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(JavaPairRDD<String, Tuple2<Integer, Integer>> rdd) throws Exception {
                //时间格式
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<Integer, Integer>>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Tuple2<Integer, Integer>>> tuple2) throws Exception {
                        while (tuple2.hasNext()) {
                            Tuple2<String, Tuple2<Integer, Integer>> tuple = tuple2.next();
                            String monitorId = tuple._1;    //卡口id
                            Integer speed = tuple._2._1;    //车速
                            Integer count = tuple._2._2;    //车的数量
                            System.out.println("卡口ID：" + monitorId + " 总车速：" + speed + " 车数量：" + count + " 平均速度：" + (speed / count));
                        }
                    }
                });
            }
        });
    }
}
