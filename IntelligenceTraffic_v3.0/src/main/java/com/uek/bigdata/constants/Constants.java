package com.uek.bigdata.constants;

/**
 * 类名称：Constants
 * 类描述：项目常量接口
 *      功能：在整个项目开发过程中，为了避免硬编码，回吧变量值尽可能的进行抽取
 *            然后封装成一个常量接口，好处就是，在项目需要修改配置时，之间修改常量接口就行
 * @author 武建谦
 * @Time 2018/11/23 15:29
 */
public interface Constants {

    /**
     * 1.关系型数据库、jdbc相关设置：
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL_PROD = "jdbc.url.prod";
    String JDBC_USER_PROD = "jdbc.user.prod";
    String JDBC_PASSWORD_PROD = "jdbc.password.prod";

    /**
     * 2.spark任务参数：
     */
    String APP_NAME_1 = "卡口车流量分析统计";
    String APP_NAME_2 = "随机抽取N个车辆信息统计分析";
    String APP_NAME_3 = "跟车分析";
    String APP_NAME_4 = "区域top3道路车流量统计分析";
    String APP_NAME_5 = "卡口车流量转化率分析";
    String APP_NAME_6 = "道路实时拥堵统计分析";
    String SPARK_LOCAL = "spark.local";
    String PARAM_START_DATE = "startDate";     //任务的开始时间
    String PARAM_END_DATE = "endDate";         //任务的结束时间
    String PARAM_MONITOR_FLOW = "roadFlow";
    String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
    String KAFKA_TOPICS = "kafka.topics";

    /**
     * 3、spark 本地运行task的参数：
     */
    String SPARK_LOCAL_TASKID_MONITORFLOW = "spark.local.taskid.monitorFlow";
    String SPARK_LOCAL_TASKID_EXTRACTCAR = "spark.local.taskid.extractCar";
    String SPARK_LOCAL_TASKID_FOLLOWCAR = "spark.local.taskid.followCar";
    String SPARK_LOCAL_TASKID_ROADFLOWTOPN = "spark.local.taskid.roadFlowTopN";
    String SPARK_LOCAL_TASKID_MONITORCARFLOWTRANSFORM = "spark.local.taskid.monitorCarFlowTransform";

    /**
     * 4、Spark作业相关的常量
     */
    String FIELD_CAMERA_COUNT = "cameraCount";
    String FIELD_CAMERA_IDS = "cameraIds";
    String FIELD_CAR_COUNT = "carCount";
    String FIELD_NORMAL_MONITOR_COUNT = "normalMonitorCount";
    String FIELD_NORMAL_CAMERA_COUNT = "normalCameraCount";
    String FIELD_ABNORMAL_MONITOR_COUNT = "abnormalMonitorCount";
    String FIELD_ABNORMAL_CAMERA_COUNT = "abnormalCameraCount";
    String FIELD_ABNORMAL_MONITOR_CAMERA_INFOS = "abnormalMonitorCameraInfos";
    String FIELD_TOP_NUM = "topNum";
    String FIELD_DATE_HOUR="dateHour";
    String FIELD_CAR_TRACK="carTrack";
    String FIELD_DATE="dateHour";
    String FIELD_CAR="carNum";
    String FIELD_CARS="cars";
    String FIELD_MONITOR="monitor";
    String FIELD_MONITOR_ID="monitorId";
    String FIELD_ACTION_TIME="actionTime";
    String FIELD_EXTRACT_NUM = "extractNum";
    //低速行驶
    String FIELD_SPEED_0_60 = "0_60";
    //正常行驶
    String FIELD_SPEED_60_90 = "60_90";
    //中速行驶
    String FIELD_SPEED_90_120 = "90_120";
    //高速行驶
    String FIELD_SPEED_120_MAX = "120_max";
    String FIELD_AREA_ID = "areaId";
    String FIELD_AREA_NAME = "areaName";
}
