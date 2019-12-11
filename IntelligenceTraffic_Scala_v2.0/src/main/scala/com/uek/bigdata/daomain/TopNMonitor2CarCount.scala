package com.uek.bigdata.daomain
import scala.beans._
/**
  * 类名称: TopNMonitor2CarCount
  * 类描述:
  *
  * @Time 2018/12/2 15:26
  * @author 武建谦
  */
class TopNMonitor2CarCount {
  @BeanProperty var taskId:Long = _
  @BeanProperty var monitorId:String = _
  @BeanProperty var count:String = _

  def this(taskId:Long,monitorId:String,count:String){
    this()
    this.taskId = taskId
    this.monitorId = monitorId
    this.count = count
  }

  override def toString = s"TopNMonitor2CarCount($taskId, $count, $monitorId)"
}
