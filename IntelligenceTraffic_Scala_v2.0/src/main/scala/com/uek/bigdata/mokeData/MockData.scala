package com.uek.bigdata.mokeData

import java.lang.Exception

import com.uek.bigdata.utils.{DateUtils, StringUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * 类名称: MockData
  * 类描述:
  *
  * @Time 2018/11/26 22:49
  * @author 武建谦
  */
object MockData {
  def mockData(sparkSession: SparkSession, sc: SparkContext): Unit = {
    /**
      * 根据实际情况生成数据字段
      * 时间 卡口号，摄像头编（id） 卡牌号 通过卡口时被拍摄的时间
      * 车牌号，车辆通过卡扣式被拍摄的时间 、车辆通过卡口的速度
      */
    //构建随机数对象，生成随机数
    val random = new Random

    //生成车牌上的省会，数据权重偏向于北京
    val location = Array[String]("川", "黑", "晋", "深", "沪", "鲁", "京", "京", "京", "京", "京", "京")
    //获取当前日期
    val date = DateUtils.getTodayDate()
    var list: ListBuffer[Row] = new ListBuffer[Row]
    //模拟生成3000辆车
    for (i <- 1 to 3000) {
      //生成车牌号
      val carNum = location(random.nextInt(location.length)) + (random.nextInt(26) + 65).toChar + StringUtils.complementStr(5, random.nextInt(99999).toString)
      //5、生成车辆通过卡口时被拍摄的时间，精确到小时  拍摄时间 = yyyy-MM-dd 00-24
      var tempActionTime = date + " " + StringUtils.complementStr(random.nextInt(24).toString)
      //6、模拟每一辆车可以做够的卡口数量 0-300，每当一辆车通过30卡口时，被拍摄的时间要加1小时
      for (i <- 0 to random.nextInt(300)) {
        if (i != 0 && i % 30 == 0) {
          tempActionTime = date + " " + StringUtils.complementStr((tempActionTime.split(" ")(1).toInt + 1).toString)
        }
      }
      //7、具体某个摄像头的拍摄时间，精确到秒 拍摄时间 = yyyy-MM-dd 00_24:00-60:00-60
      val actionTime = tempActionTime + ":" + StringUtils.complementStr(random.nextInt(60).toString) + ":" + StringUtils.complementStr(random.nextInt(60).toString)

      //8、生成卡口id 0000-0009
      val monitorId = StringUtils.complementStr(4, random.nextInt(9).toString)
      //9、生成车辆通过卡口的速度 0 -260
      val speed = StringUtils.complementStr(random.nextInt(260).toString)
      //10、生成道路id  1 - 50
      val roadId = StringUtils.complementStr(random.nextInt(50).toString)
      //11、生成摄像头id 00000-99999
      val cameraId = StringUtils.complementStr(5, random.nextInt(15).toString)
      //12、生成区域id： 00-08
      val areaId = StringUtils.complementStr(random.nextInt(8).toString)
      val row = RowFactory.create(date, monitorId, cameraId, carNum, actionTime, speed, roadId, areaId)
      list += (row)
    }
    var rowRDD = sc.parallelize(list)
    val structType = StructType(
      new StructField("date", StringType) ::
        new StructField("monitorId", StringType) ::
        new StructField("cameraId", StringType) ::
        new StructField("carNum", StringType) ::
        new StructField("actionTime", StringType) ::
        new StructField("speed", StringType) ::
        new StructField("roadId", StringType) ::
        new StructField("areaId", StringType) :: Nil
    )

    val dateSet = sparkSession.createDataFrame(rowRDD, structType)
    dateSet.createOrReplaceTempView("monitor_flow_action")
    dateSet.show()

    // TODO: 生成数据字典标准数据 记录每个卡口下的摄像头信息
    /**
      * 卡口id  摄像头ID
      */
    //定义标准数据的数据结构
    var map = new scala.collection.mutable.HashMap[String, mutable.HashSet[String]]
    //记录车辆的信息
    var count: Int = 0;
    //通过遍历获取所有车的信息
    for (row <- list) {
      val monitorId = row.getString(1)
      var set: mutable.HashSet[String] = null
      try {
        set = map(monitorId)
      } catch {
        case e: Exception =>
          set = new mutable.HashSet[String]
          map += (monitorId -> set)
      }
      val cameraId = row.getString(2)
      set += (cameraId)
      //车辆数加1
      count += 1

      //保证标准的数据要比实际数据要多一条
      if (count % 1000 == 0) {
        set += (StringUtils.complementStr(5, random.nextInt(999).toString))
      }
    }
    //对list列表清空，第二次使用
    list.clear()
    for ((k, v) <- map) {
      for (elem <- v) {
        val row = RowFactory.create(k, elem)
        list += (row)
      }
    }
    //构建rowRDD
    rowRDD = sc.parallelize(list)
    //自定义schema
    val structType1 = StructType(
      new StructField("monitorId", StringType) ::
        new StructField("cameraId", StringType) :: Nil
    )
    val dateSet1 = sparkSession.createDataFrame(rowRDD, structType1)
    dateSet1.createOrReplaceTempView("monitor_camera_info")
    dateSet1.show()

    //生成北京市各区名称
    val areas = Array[String]("朝阳区", "海定区", "西域区", "丰谷区", "吕平区", "东城区", "顺义区", "大兴区")

    //给区域添加区号
    list.clear()
    val row: Row = null;
    for (i <- 0 until areas.length) {
      val row = RowFactory.create(i:Integer, areas(i))
      list += (row)
    }
    val structType2 = StructType(
      new StructField("areas_id", IntegerType) ::
        new StructField("area", StringType) :: Nil
    )
    rowRDD = sc.parallelize(list);
    val dateSet2 = sparkSession.createDataFrame(rowRDD,structType2)
    dateSet2.createOrReplaceTempView("area_info")
    dateSet2.show()
  }
}
