package com.uek.bigdata.mockData;

import com.uek.bigdata.utils.DateUtils;
import com.uek.bigdata.utils.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * 类名称：${class}
 * 类描述：
 * 生成模拟数据
 *
 * @author 武建谦
 * @Time 2018/11/25 9:03
 */
public class MockData {
    public static void mockData(SparkSession sparkSession, JavaSparkContext jsc){
        /**
         * 生成实际情况的数据
         * 字段 时间 卡口号，摄像头编（id） 卡牌号 卖出了通过卡口时被拍摄的时间
         * 车牌号，车辆通过卡扣式被拍摄的时间 、车辆通过卡口的速度
         */

        //
        List<Row> list = new ArrayList<>();

        //构建随机数对象，生成随机数
        Random random = new Random();

        //1、生成车牌号上的省会简称，因为模拟的是背景交通情况，所以在数据的权重上偏向于北京的省会简称、
        String[] location = {"川", "黑", "晋", "深", "沪", "鲁", "京", "京", "京", "京", "京", "京"};
        //2、获取当前的日期
        String date = DateUtils.getTodayDate();
        //3、模拟生成3000辆汽车
        int i = 0;
        while (i < 3000) {
            //4、车牌号=省会简称 + a-z + 00000-99999
            String carName = location[random.nextInt(location.length - 1)] + (char) (65 + random.nextInt(26)) +
                    StringUtils.complementStr(5, String.valueOf(random.nextInt(99999)));
            //5、生成车辆通过卡口时被拍摄的时间，精确到小时  拍摄时间 = yyyy-MM-dd 00-24
            String tempActionTime = date + " " + StringUtils.complementStr(String.valueOf(random.nextInt(24)));

            //6、模拟每一辆车可以做够的卡口数量 0-300，每当一辆车通过30卡口时，被拍摄的时间要加1小时
            for (int j = 0; j < random.nextInt(300); j++) {
                if (j != 0 && j % 30 == 0) {
                    tempActionTime = date + " " + StringUtils.complementStr(String.valueOf(Integer.parseInt(tempActionTime.split(" ")[1]) + 1));
                }
            }
            //7、具体某个摄像头的拍摄时间，精确到秒 拍摄时间 = yyyy-MM-dd 00_24:00-60:00-60
            String actionTime = tempActionTime + ":" + StringUtils.complementStr(String.valueOf(random.nextInt(60)))
                    + ":" + StringUtils.complementStr(String.valueOf(random.nextInt(60)));
            //8、生成卡口id 0000-0009
            String monitorId = StringUtils.complementStr(4, String.valueOf(random.nextInt(9)));
            //9、生成车辆通过卡口的速度 0 -260
            String speed = String.valueOf(random.nextInt(260));
            //10、生成道路id  1 - 50
            String roadId = String.valueOf(random.nextInt(49) + 1);

            //11、生成摄像头id 00000-99999
            String cameraId = StringUtils.complementStr(5, String.valueOf(random.nextInt(32)));

            //12、生成区域id： 00-08
            String areaId = StringUtils.complementStr(String.valueOf(random.nextInt(8)));

            //将所有生成的数据封装成row对象
            //时间 卡口号，摄像头编（id） 卡牌号 卖出了通过卡口时被拍摄的时间
            //车牌号，车辆通过卡扣式被拍摄的时间 、车辆通过卡口的速度,道路id ，区域id
            Row row = RowFactory.create(date, monitorId, cameraId, carName, actionTime, speed, roadId, areaId);
            //每个row对象代表一辆车
            list.add(row);
            i++;
        }
        //构建实际情况RDD数据
        JavaRDD<Row> rowRdd = jsc.parallelize(list);

        //
        StructType structType = DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("date",DataTypes.StringType,true),
                        DataTypes.createStructField("monitor_id",DataTypes.StringType,true),
                        DataTypes.createStructField("camera_id",DataTypes.StringType,true),
                        DataTypes.createStructField("car_num",DataTypes.StringType,true),
                        DataTypes.createStructField("action_time",DataTypes.StringType,true),
                        DataTypes.createStructField("speed",DataTypes.StringType,true),
                        DataTypes.createStructField("road_id",DataTypes.StringType,true),
                        DataTypes.createStructField("area_id",DataTypes.StringType,true)
                )
        );
        Dataset<Row> dataset = sparkSession.createDataFrame(rowRdd, structType);
        dataset.createOrReplaceTempView("monitor_flow_action");
        Dataset<Row> sql = sparkSession.sql("select * from monitor_flow_action");
        sql.show();

        /**
         * 生成数据字典标准的数据
         * 2个字段数据
         * 卡口id 和摄像头id
         * 注意：因为这个操作都是使用模拟数据，而不是真实数据，所以数据字典的标准数据是依据实际数据来构建的
         */
        //这个数据结构是来存储标准数据
        //String 卡口id   Set<String> 卡口id对应下的所有摄像头id集合
        Map<String,Set<String>> map = new HashMap<>();
        //记录车辆的信息
        int count = 0;
        //通过变量所有车辆信息
        for (Row row : list) {
            //获取卡口id
            String monitorId = row.getString(1);
            //因为需要把卡口id对应的摄像头id全部存储在set中，所以首先需要判断set中是否添加过仙童的摄像头id（避免重复）
            Set<String> set = map.get(monitorId);
            //初始化操作
            if (set == null) {
                set = new HashSet<>();
                map.put(monitorId,set);
            }
            //记录车辆信息的数量
            count ++;
            //获取摄像头id
            String cameraId = row.getString(2);
            //通过遍历，把一个卡口下对应的所有摄像头id添加进去
            set.add(cameraId);

            /**
             * 通过额外添加一条数据，来保证标准时间永远要比实际的数据要多一条
             * 因为在需求中我们要对两种数据进行比较，如果完全一样就无法比较
             * 需求：统计一个卡口下要统计那些摄像头坏了。如果出现故障的数据和标准数据进行比较式
             */

            if (count % 1000 == 0) {
                set.add(StringUtils.complementStr(5, String.valueOf(random.nextInt(999))));
            }
        }
        //对每一个卡口下的摄像头id进行拆分，以卡口 id做为key，，摄像头id作为value来封装row对象，让后添加到集合中
        list.clear();
        for (Map.Entry<String, Set<String>> entrySet : map.entrySet()) {
            String monitorId = entrySet.getKey();
            Set<String> cameraIds = entrySet.getValue();

            Row row = null;
            for (String cameraId : cameraIds) {
                row = RowFactory.create(monitorId,cameraId);
                list.add(row);
            }
        }

        rowRdd = jsc.parallelize(list,10);
        //
        StructType structType1 = DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("monitor_id",DataTypes.StringType,true),
                        DataTypes.createStructField("camera_id",DataTypes.StringType,true)
                )
        );
        Dataset<Row> dataset1 = sparkSession.createDataFrame(rowRdd, structType1);
        dataset1.createOrReplaceTempView("monitor_camera_info");
        Dataset<Row> sql1 = sparkSession.sql("select * from monitor_camera_info");
        sql1.show();


        //生成北京市各区名称
        String[] areas = new String[]{"朝阳区","海定区","西域区","丰谷区","吕平区","东城区","顺义区","大兴区"};

        //给区域名添加区号
        list.clear();
        Row row = null;
        for (int j = 0; j < areas.length ; j++) {
            row = RowFactory.create(j+"" , areas[j]);
            list.add(row);
        }

        rowRdd = jsc.parallelize(list);
        StructType structType2 = DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("areas_id",DataTypes.StringType,true),
                        DataTypes.createStructField("areas",DataTypes.StringType,true)
                )
        );
        Dataset<Row> dataSet2 = sparkSession.createDataFrame(rowRdd, structType2);
        dataSet2.createOrReplaceTempView("area_info");
        dataSet2.show();
    }
}
