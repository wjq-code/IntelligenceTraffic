package com.uek.bigdata.model

import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.parser.deserializer.JSONObjectDeserializer
import com.uek.bigdata.DAO.factory.DAOFactory
import com.uek.bigdata.config.ConfigurationManager
import com.uek.bigdata.constans.Constants
import com.uek.bigdata.daomain.{MonitorState, SpeedSortKey, TopNMonitor2CarCount, TopNMonitorDetailInfo}
import com.uek.bigdata.model.accumulate.MonitorAndCameraStatusAccumulate
import com.uek.bigdata.mokeData.MockData
import com.uek.bigdata.utils.{ParamsUtils, StringUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.api.java.JavaPairInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.ListBuffer

/**
  * 类名称: MonitorCarFlowAnalyze
  * 类描述:
  *
  * @Time 2018/12/1 9:18
  * @author 武建谦
  */
object MonitorCarFlowAnalyze {

  def main(args: Array[String]): Unit = {
    //获取配置对象
    val conf = new SparkConf()
    //判断是否是本地运行状态,
    val flag = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (flag) {
      //本地运行状态
      conf.setMaster("local[*]")
    }
    //创建sparkSession对象
    val sparkSession = SparkSession.builder()
      .appName(Constants.APP_NAME_1)
      .config(conf)
      .getOrCreate()
    //创建sparkContext对象
    val sc = sparkSession.sparkContext
    //设置日志的输出级别
    sc.setLogLevel("WARN")
    //生成数据
    MockData.mockData(sparkSession, sc)

    // TODO: 功能模块一：卡口车流量分析
    //获取taskId
    val taskId = ParamsUtils.getTaskId(null, Constants.SPARK_LOCAL_TASKID_MONITORFLOW)
    //功能一种用户提交的参数
    val taskDao = DAOFactory.getTaskDao()
    val task = taskDao.selectTaskById(taskId)
    val jsonStr = task.getTaskParams
    // TODO: 1.1：查询指定时间范围内的卡口数据
    //1、获取到指定的时间范围内的卡口数据
    var monitorInfoRDD = getMonitorInfoByDataRange(sparkSession, jsonStr)
    //缓存
    monitorInfoRDD = monitorInfoRDD.cache()
    //2、将数据类型该为kv格式的数据
    var monitorInfoKVRDD = getMonitorInfoKVByMonitorInfoRDD(monitorInfoRDD)
    monitorInfoKVRDD = monitorInfoKVRDD.cache()
    //3、对kv格式的数据根据卡口id进行分组
    var monitorInfoKVRDDByGroup = monitorInfoKVRDD.groupByKey()
    monitorInfoKVRDDByGroup = monitorInfoKVRDDByGroup.cache()
    //4、对分组后的数据进行聚合统计
    var monitorInfoKVByTrim = getMonitorInfoKVByTrim(monitorInfoKVRDDByGroup)
    monitorInfoKVByTrim = monitorInfoKVByTrim.cache()
    monitorInfoKVByTrim.foreach(println)
    // TODO: 1.2：检测各个卡口的运行状态是否正确
    //注册累加器
    val accum = new MonitorAndCameraStatusAccumulate
    sc.register(accum, ",myAccum")
    println("功能二：")
    //检测各卡口的状态
    var carCountByMonitorInfos = checkMonitorState(sc, sparkSession, accum, monitorInfoKVByTrim)
    carCountByMonitorInfos = carCountByMonitorInfos.cache()
    carCountByMonitorInfos.foreach(println)
    //将记录的卡口状态记录到数据库中
    insertAccumulatorResultToMySQL(taskId,accum)
    //TODO 获取车流排名前N的卡口号，并把结果插入到数据库
    var topNMonitor2CarFlow = getMonitorCarFlowTopN(sc,carCountByMonitorInfos,taskId,jsonStr)
    topNMonitor2CarFlow = topNMonitor2CarFlow.cache()
    // TODO 1.3 获取车流量最大的topN卡口的详细信息，并且持久化到数据库中

    getTopNDetails(taskId,topNMonitor2CarFlow,monitorInfoKVRDD)


    // TODO 1.4 获取经常被高速通过的Top5卡口id
    speedTopNMonitor(monitorInfoKVRDDByGroup)
    println(accum.value)
    sc.stop()
    sparkSession.stop()

  }

  /**
    * 获取经常被高速通过的Top5卡口Id
    * @param monitorInfoKVRDDByGroup
    */
  def speedTopNMonitor(monitorInfoKVRDDByGroup: RDD[(String, Iterable[Row])]) ={
   val groupByMonitorId = monitorInfoKVRDDByGroup.map(tuple2 =>{
      val monitor = tuple2._1
      val iter = tuple2._2.iterator
      var lowSpeed: Long = 0
      var normalSpeed: Long = 0
      var mediumSpeed: Long = 0
      var highSpeed: Long = 0
      while (iter.hasNext) {
        val row = iter.next()
        val speed:Int = row.getString(5).toInt
        //判断速度
        if (speed >= 0 && speed < 60) { //低速
          lowSpeed += 1
        }
        else if (speed >= 60 && speed <= 90) { //正常
          normalSpeed += 1
        }
        else if (speed >= 90 && speed < 120) { //中速
          mediumSpeed += 1
        }
        else if (speed >= 120) { //高速
          highSpeed += 1
        }
      }
      val speedSotrKey = SpeedSortKey(lowSpeed,normalSpeed,mediumSpeed,highSpeed)
      (speedSotrKey,monitor)
    })
//    groupByMonitorId.sortByKey(false,1)


  }


  /**
    * 获取车流量最大的topN卡口的详细信息，并且持久化到数据库中
    * @param taskId
    * @param topNMonitor2CarFlow
    * @param monitorInfoKVRDD
    * @return
    */
  def getTopNDetails(taskId: Long, topNMonitor2CarFlow: RDD[(String, Integer)], monitorInfoKVRDD: RDD[(String, Row)])={
    val joinRdd = topNMonitor2CarFlow.join(monitorInfoKVRDD)
    val mapByJoinRdd  = joinRdd.map(tuple2 =>{
      (tuple2._1,tuple2._2._2)
    })
    val list = new ListBuffer[TopNMonitorDetailInfo]
    mapByJoinRdd.foreachPartition(iter =>{
      var monitorId = ""
      while (iter.hasNext) {
        val tuple2 = iter.next()
        monitorId = tuple2._1
        val row = tuple2._2
        val topNMonitorDetailInfo = new TopNMonitorDetailInfo(
          taskId,
          row.getString(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getString(5),
          row.getString(6)
        )
        list += topNMonitorDetailInfo
      }
      val monitor = DAOFactory.getMonitor()
      monitor.insertTopNMonitorDetailInfo(list)
    })
  }



  /**
    * 把车流量前N的信息插入到数据库中
    * @param carCountByMonitorInfos
    * @param taskId
    */
  def getMonitorCarFlowTopN(sc:SparkContext,carCountByMonitorInfos: RDD[(Integer, String)], taskId: Long,jsonStr:String):RDD[(String,Integer)] = {
    //获取用户需要的TopN
    val num = ParamsUtils.getParamsByJsonStr(jsonStr,Constants.FIELD_TOP_NUM)
    val topNCarCount = carCountByMonitorInfos.sortByKey(false,1).take(num.toInt)
    val list = new ListBuffer[TopNMonitor2CarCount]
    for (tuple2 <- topNCarCount) {
      val count = tuple2._1
      val monitorId = tuple2._2
      val obj = new TopNMonitor2CarCount(taskId,monitorId,count.toString)
      list+=(obj)
    }
    val monitorDao = DAOFactory.getMonitor()
    monitorDao.insertTopNMonitor2CarCount(list)

    /**
      * 封装数据为下文做准备
      */
    val monitorId2MonitorIdRDD = sc.parallelize(topNCarCount)
    monitorId2MonitorIdRDD.map(_.swap)
  }


  /**
    * 将累加器记录的五种状态添加到数据库中
    * @param taskId 任务的id值
    * @param accum 累加器
    * @return
    */
  def insertAccumulatorResultToMySQL(taskId: Long, accum: MonitorAndCameraStatusAccumulate) = {
    val accumulatorVal = accum.value
    val normalMonitorCount = StringUtils.extractValue(accumulatorVal, "|", Constants.FIELD_NORMAL_MONITOR_COUNT)
    val normalCameraCount = StringUtils.extractValue(accumulatorVal, "|", Constants.FIELD_NORMAL_CAMERA_COUNT)
    val abnormalMonitorCount = StringUtils.extractValue(accumulatorVal, "|", Constants.FIELD_ABNORMAL_MONITOR_COUNT)
    val abnormalCameraCount = StringUtils.extractValue(accumulatorVal, "|", Constants.FIELD_ABNORMAL_CAMERA_COUNT)
    val abnormalMonitorCameraInfos = StringUtils.extractValue(accumulatorVal, "|", Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS)
    val monitorState = new MonitorState(taskId,normalMonitorCount,normalCameraCount,abnormalMonitorCount,abnormalCameraCount,abnormalMonitorCameraInfos)
    val monitorDao = DAOFactory.getMonitor()
    monitorDao.insertMonitorState(monitorState)
  }


  /**
    * 功能1.2 对各个卡口的运行状态进行统计
    *
    * @param sc
    * @param sparkSession
    * @param accum
    * @param monitorInfoKVByTrim
    * @return
    */
  def checkMonitorState(sc: SparkContext, sparkSession: SparkSession, accum: MonitorAndCameraStatusAccumulate, monitorInfoKVByTrim: RDD[(String, String)]) = {
    //获取标准数据字典表中的数据
    val sql = "select * from monitor_camera_info"
    val dataSet = sparkSession.sql(sql)
    val rowRdd = dataSet.rdd
    val kvRdd = rowRdd.map(row => {
      val monitorId = row.getString(0)
      val cameraId = row.getString(1)
      (monitorId, cameraId)
    })
    val kvRddByGroup = kvRdd.groupByKey()
    val standDataBykvRddByGroup = kvRddByGroup.map(tuple2 => {
      val monitorId = tuple2._1
      val iter = tuple2._2.iterator
      val sb = new StringBuilder
      var count = 0
      while (iter.hasNext) {
        val cameraId = iter.next()
        sb.append(",").append(cameraId)
        count += 1
      }
      val cameraInfo = Constants.FIELD_CAMERA_IDS + "=" + sb.substring(1) + "|" +
        Constants.FIELD_CAMERA_COUNT + "=" + count
      (monitorId, cameraInfo)
    })

    //使用标准的数据和实际的数据进行leftjoin
    val joinRdd = standDataBykvRddByGroup.leftOuterJoin(monitorInfoKVByTrim)

    val carCountByMonitorInfos = joinRdd.mapPartitions(iter => {
      //存储最终数据
      val list = new ListBuffer[Tuple2[Integer, String]]
      while (iter.hasNext) {
        val tuple2 = iter.next()
        val monitorId = tuple2._1
        val standardCameraInfos = tuple2._2._1
        val practicalCameraInfos = tuple2._2._2
        var practicalCameraInfosStr = "" //实际情况卡口下摄像头信息
        //第一种情况：有变没有数据
        import scala.util.control.Breaks._
        breakable {
          if (practicalCameraInfos.isEmpty) {
            //异常摄像头信息

            val cameraIds = StringUtils.extractValue(standardCameraInfos, "|", Constants.FIELD_CAMERA_IDS)
            val cameraCount = StringUtils.extractValue(standardCameraInfos, "|", Constants.FIELD_CAMERA_COUNT)
            val newStr = Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|" +
              Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + cameraCount + "|" +
              Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + cameraIds
            accum.add(newStr)
            break()
          } else {
            //option中存储的数据
            practicalCameraInfosStr = practicalCameraInfos.get
          }
        }
        //获取表准数据中摄像头的个数
        println("standardCameraInfos:  "+standardCameraInfos)
        val standardCameraCount = StringUtils.extractValue(standardCameraInfos, "|", Constants.FIELD_CAMERA_COUNT).toInt
        val practicalCameraCount = StringUtils.extractValue(practicalCameraInfosStr, "|", Constants.FIELD_CAMERA_COUNT).toInt
        if (standardCameraCount == practicalCameraCount) {
          //没有损坏的摄像头和卡口
          val newStr = Constants.FIELD_NORMAL_MONITOR_COUNT + "=1|" +
            Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + standardCameraCount + "|"
          accum.add(newStr)
        } else {
          //摄像头的信息
          //标准
          val standardCameraIds = StringUtils.extractValue(standardCameraInfos, "|", Constants.FIELD_CAMERA_IDS)
          val standardCameraIdList = standardCameraIds.split(",").toList
          //实际
          val practicalCameraIds = StringUtils.extractValue(practicalCameraInfosStr, "|", Constants.FIELD_CAMERA_IDS)
          val practicalCameraIdList = practicalCameraIds.split(",").toList
          var abnormalCmeraCount = 0
          var normalCameraCount = 0
          val abCamre = new ListBuffer[String]
          for (standCameraId <- standardCameraIdList) {
            if (practicalCameraIdList.contains(standCameraId)) {
              normalCameraCount += 1
            }
            abCamre += (standCameraId)
            abnormalCmeraCount += 1
          }
          val newStr = Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + normalCameraCount + "|" +
            Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|" +
            Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnormalCmeraCount + "|" +
            Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + abCamre.mkString(",")
          accum.add(newStr)
        }
        //从实际数据中获取当前卡口下的信息
        val carCount = StringUtils.extractValue(practicalCameraInfosStr,"|",Constants.FIELD_CAR_COUNT).toInt
        list+=(new Tuple2[Integer,String](carCount,monitorId))
      }
      list.iterator
    })
    carCountByMonitorInfos
  }


  /**
    * 功能1.1.1 在原始数据中获取指定时间段的数据
    *
    * @param sparkSession
    * @param jsonStr
    * @return
    */
  def getMonitorInfoByDataRange(sparkSession: SparkSession, jsonStr: String): RDD[Row] = {
    //获取用户指定的开始时间
    val startTime = ParamsUtils.getParamsByJsonStr(jsonStr, Constants.PARAM_START_DATE)
    //获取用户指定的结束时间
    val endTime = ParamsUtils.getParamsByJsonStr(jsonStr, Constants.PARAM_END_DATE)
    val sql = "SELECT * " +
      "FROM monitor_flow_action " +
      "WHERE date >= '" + startTime + "' AND date <= '" + endTime + "' "
    val dateSet = sparkSession.sql(sql)
    dateSet.rdd
  }

  /**
    * 功能1.1.2 对指定了时间段的数据，将它转换为kv格式
    *
    * @param monitorInfoRDD 指定时间段的数据
    * @return kv格式的RDD
    */
  def getMonitorInfoKVByMonitorInfoRDD(monitorInfoRDD: RDD[Row]): RDD[(String, Row)] = {
    val monitorInfoKVRDD = monitorInfoRDD.map(row => {
      val monitorId = row.getString(1)
      (monitorId, row)
    })
    monitorInfoKVRDD
  }

  /**
    * 对每个卡口下的数据做聚合统计
    *
    * @param monitorInfoKVRDDByGroup 分组之后的rdd
    * @return
    */
  def getMonitorInfoKVByTrim(monitorInfoKVRDDByGroup: RDD[(String, Iterable[Row])]): RDD[(String, String)] = {
    monitorInfoKVRDDByGroup.map(tuple2 => {
      //获取摄像头Id
      val monitorId = tuple2._1
      val iter = tuple2._2.iterator
      //获取row对象中的内容，进行拼接
      //用list来存放摄像头的信息
      val list = new ListBuffer[String]
      //定义sb来拼接摄像头信息
      val sb = StringBuilder.newBuilder
      //记录区域Id
      var areaId = ""
      //记录车辆信息
      var count = 0
      while (iter.hasNext) {
        //每一辆车的row对象
        val row = iter.next()
        //获取区域id
        areaId = row.getString(7)
        //获取摄像头id
        val cameraIds = row.getString(2)
        if (!list.contains(cameraIds)) {
          list += (cameraIds)
        }
        //车辆数加1
        count += 1
      }
      //拼接摄像头
      for (cameraId <- list) {
        sb.append(",").append(cameraId)
      }
      val cameraCount = list.size
      //拼接字符串
      val monitorIdInfo = Constants.FIELD_MONITOR_ID + "=" + monitorId + "|" +
        Constants.FIELD_CAMERA_IDS + "=" + sb.substring(1).toString + "|" +
        Constants.FIELD_CAMERA_COUNT + "=" + cameraCount + "|" +
        Constants.FIELD_CAR_COUNT + "=" + count
      (monitorId, monitorIdInfo)
    })
  }
}
