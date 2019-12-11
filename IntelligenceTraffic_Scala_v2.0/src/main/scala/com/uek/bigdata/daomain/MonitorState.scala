package com.uek.bigdata.daomain
import scala.beans.BeanProperty
/**
  * 类名称: MonitorState
  * 类描述:
  *
  * @Time 2018/12/1 21:54
  * @author 武建谦
  */
class MonitorState {
  @BeanProperty var taskId:Long = 0L
  @BeanProperty var noramlMonitorCount:String = null
  @BeanProperty var normalCameraCount:String = null
  @BeanProperty var abnormalMonitorCount:String = null
  @BeanProperty var abnormalCameraCount:String = null
  @BeanProperty var abnormalMonitorCameraInfos:String = null

  def this(taskId:Long,noramlMonitorCount:String,normalCameraCount:String,abnormalMonitorCount:String,abnormalCameraCount:String,abnormalMonitorCameraInfos:String){
    this()
    this.taskId = taskId
    this.noramlMonitorCount = noramlMonitorCount
    this.normalCameraCount = normalCameraCount
    this.abnormalMonitorCount = abnormalMonitorCount
    this.abnormalCameraCount = abnormalCameraCount
    this.abnormalMonitorCameraInfos = abnormalMonitorCameraInfos
  }

  override def toString = s"MonitorState($taskId, $noramlMonitorCount, $normalCameraCount, $abnormalMonitorCount, $abnormalCameraCount, $abnormalMonitorCameraInfos)"
}
