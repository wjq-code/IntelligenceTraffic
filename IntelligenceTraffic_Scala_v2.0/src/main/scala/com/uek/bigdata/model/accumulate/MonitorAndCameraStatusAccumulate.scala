package com.uek.bigdata.model.accumulate

import com.uek.bigdata.constans.Constants
import com.uek.bigdata.utils.StringUtils
import org.apache.spark.util.AccumulatorV2

/**
  * 类名称: MonitorAndCameraStatusAccumulate
  * 类描述: 用来记录这个卡口的五种状态
  *
  * @Time 2018/12/1 12:50
  * @author 武建谦
  */
/**
  * AccumulatorV2[String,String]
  * 第一个String是输入的类型
  * 第二个String是输出的类型
  */
class MonitorAndCameraStatusAccumulate extends AccumulatorV2[String, String] {
  //定义五种累加器的状态初始值
  private var statusStr = Constants.FIELD_NORMAL_MONITOR_COUNT + "=0|" +
    Constants.FIELD_NORMAL_CAMERA_COUNT + "=0|" +
    Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=0|" +
    Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=0|" +
    Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "= "

  /**
    * 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序。
    * 累加器中的数据结构是否为空
    *
    * @return
    */
  override def isZero: Boolean = true

  /**
    * 拷贝累加器，分发给不同的worker的executor
    *
    * @return
    */
  override def copy(): AccumulatorV2[String, String] = {
    val newAccumulator = new MonitorAndCameraStatusAccumulate
    newAccumulator.statusStr = this.statusStr
    newAccumulator
  }

  /**
    * 重置累加器中的数据
    */
  override def reset(): Unit = {
    statusStr = Constants.FIELD_NORMAL_MONITOR_COUNT + "=0|" +
      Constants.FIELD_NORMAL_CAMERA_COUNT + "=0|" +
      Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=0|" +
      Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=0|" +
      Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "= "
  }

  /**
    * 核心方法：实现累加功能
    *
    * @param newStr 传入的新字符串
    */
  override def add(newStr: String): Unit = {
    //对传入的字符串进行切割
    val KvStr = newStr.split("\\|")
    for (str <- KvStr) {
      val kvs = str.split("=")
      val key = kvs(0)
      val value = kvs(1)
      //如果key值是拼接字符串的
      if (key == Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS) {
        //获取旧数据
        val oldValue = StringUtils.extractValue(statusStr, "|", key)
        //拼接新的数据
        statusStr = StringUtils.setFieldValue(statusStr, "|", key, oldValue + "-" + value)
      } else {
        val oldValue = StringUtils.extractValue(statusStr, "|", key).toInt
        val newValue = value.toInt + oldValue
        statusStr = StringUtils.setFieldValue(statusStr, "|", key, newValue.toString)
      }
    }
  }

  /**
    * 不同分区中的合并操作
    *
    * @param other
    */
  override def merge(other: AccumulatorV2[String, String]): Unit = other match {
    case o: MonitorAndCameraStatusAccumulate => {
      val newStr = o.statusStr
      //对传入的字符串进行切割
      val KvStr = newStr.split("\\|")
      for (str <- KvStr) {
        val kvs = str.split("=")
        val key = kvs(0)
        val value = kvs(1)
        //如果key值是拼接字符串的
        if (key == Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS) {
          //获取旧数据
          val oldValue = StringUtils.extractValue(statusStr, "|", key)
          //拼接新的数据
          this.statusStr = StringUtils.setFieldValue(statusStr, "|", key, oldValue + "-" + value)
        } else {
          val oldValue = StringUtils.extractValue(statusStr, "|", key).toInt
          val newValue = value.toInt + oldValue
          this.statusStr = StringUtils.setFieldValue(statusStr, "|", key, newValue.toString)
        }
      }
    }
    case _ => throw new UnsupportedOperationException(s"Cannot Match ${this.getClass.getName} with ${other.getClass.getName}")
  }

  /**
    * 获取累加器的结果
    *
    * @return
    */
  override def value: String = statusStr
}
