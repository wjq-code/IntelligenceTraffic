package com.uek.bigdata.constans

/**
  * 类名称: Constants
  * 类描述:项目常量接口
  * *      功能：在整个项目开发过程中，为了避免硬编码，回吧变量值尽可能的进行抽取
  * *            然后封装成一个常量接口，好处就是，在项目需要修改配置时，之间修改常量接口就行
  * @Time 2018/11/28 22:38
  * @author 武建谦
  */
object Constants {
  val JDBC_DRIVER = "jdbc.driver"
  val JDBC_URL = "jdbc.url"
  val JDBC_USER = "jdbc.user"
  val JDBC_PASSWORD = "jdbc.password"
  val JDBC_DATASOURCE_SIZE = "jdbc.datasource.size"

  /**
    * 2.spark任务参数：
    */
  val APP_NAME_1 = "卡口车流量分析统计"
  val SPARK_LOCAL = "spark.local"
  val PARAM_START_DATE = "startDate" //任务的开始时间

  val PARAM_END_DATE = "endDate" //任务的结束时间


  /**
    * 3、spark 本地运行task的参数：
    */
  val SPARK_LOCAL_TASKID_MONITORFLOW = "spark.local.taskid.monitorFlow"
  val SPARK_LOCAL_TASKID_EXTRACTCAR = "spark.local.taskid.extractCar"
  val SPARK_LOCAL_TASKID_FOLLOWCAR = "spark.local.taskid.followCar"
  val SPARK_LOCAL_TASKID_ROADFLOWTOPN = "spark.local.taskid.roadFlowTopN"
  val SPARK_LOCAL_TASKID_ROADTRANSFORM = "spark.local.taskid.roadTransform"

  /**
    * 4、Spark作业相关的常量
    */
  val FIELD_CAMERA_COUNT = "cameraCount"
  val FIELD_CAMERA_IDS = "cameraIds"
  val FIELD_CAR_COUNT = "carCount"
  val FIELD_NORMAL_MONITOR_COUNT = "normalMonitorCount"
  val FIELD_NORMAL_CAMERA_COUNT = "normalCameraCount"
  val FIELD_ABNORMAL_MONITOR_COUNT = "abnormalMonitorCount"
  val FIELD_ABNORMAL_CAMERA_COUNT = "abnormalCameraCount"
  val FIELD_ABNORMAL_MONITOR_CAMERA_INFOS = "abnormalMonitorCameraInfos"
  val FIELD_TOP_NUM = "topNum"
  val FIELD_DATE_HOUR = "dateHour"
  val FIELD_CAR_TRACK = "carTrack"
  val FIELD_DATE = "dateHour"
  val FIELD_CAR = "car"
  val FIELD_CARS = "cars"
  val FIELD_MONITOR = "monitor"
  val FIELD_MONITOR_ID = "monitorId"
  val FIELD_ACTION_TIME = "actionTime"
  val FIELD_EXTRACT_NUM = "extractNum"
  //低速行驶
  val FIELD_SPEED_0_60 = "0_60"
  //正常行驶
  val FIELD_SPEED_60_90 = "60_90"
  //中速行驶
  val FIELD_SPEED_90_120 = "90_120"
  //高速行驶
  val FIELD_SPEED_120_MAX = "120_max"
  val FIELD_AREA_ID = "areaId"
  val FIELD_AREA_NAME = "areaName"
}
