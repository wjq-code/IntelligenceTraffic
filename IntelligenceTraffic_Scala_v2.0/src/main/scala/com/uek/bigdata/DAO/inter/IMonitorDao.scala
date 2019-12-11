package com.uek.bigdata.DAO.inter

import com.uek.bigdata.daomain.{MonitorState, TopNMonitor2CarCount, TopNMonitorDetailInfo}

import scala.collection.mutable.ListBuffer

/**
  * 类名称: IMonitorStateDao
  * 类描述:
  *
  * @Time 2018/12/1 22:11
  * @author 武建谦
  */
trait IMonitorDao {
  /**
    * 将自定义累加器记录的五种状态插入到数据库：
    *
    * @param monitorState
    */
  def insertMonitorState(monitorState: MonitorState): Unit

  /**
    * 获取车流量topN的卡口
    * @param topns
    */
  def insertTopNMonitor2CarCount(topns:ListBuffer[TopNMonitor2CarCount]) : Unit

  /**
    * 将topN卡口中的详细信息插入到数据库中
    * @param topNMonitorDetailInfos
    */
  def insertTopNMonitorDetailInfo(topNMonitorDetailInfos:ListBuffer[TopNMonitorDetailInfo]) : Unit

}
