package com.uek.bigdata.daomain

import scala.beans._

/**
  * 类名称: TopNMonitorDetailInfo
  * 类描述:
  *
  * @Time 2018/12/2 16:43
  * @author 武建谦
  */
class TopNMonitorDetailInfo {
  @BeanProperty var taskId: Long = _
  @BeanProperty var date: String = _
  @BeanProperty var monitorId: String = _
  @BeanProperty var cameraId: String = _
  @BeanProperty var car: String = _
  @BeanProperty var actionTime: String = _
  @BeanProperty var speed: String = _
  @BeanProperty var roadId: String = _

  def this(taskId: Long, date: String, monitorId: String, cameraId: String, car: String, actionTime: String, speed: String, roadId: String) {
    this()
    this.taskId = taskId
    this.date = date
    this.monitorId = monitorId
    this.cameraId = cameraId
    this.car = car
    this.actionTime = actionTime
    this.speed = speed
    this.roadId = roadId
  }

  override def toString = s"TopNMonitorDetailInfo($taskId, $date, $monitorId, $cameraId, $car, $actionTime, $speed, $roadId)"
}
