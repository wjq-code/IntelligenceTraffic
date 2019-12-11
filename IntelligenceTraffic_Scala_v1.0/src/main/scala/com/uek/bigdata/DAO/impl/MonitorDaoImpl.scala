package com.uek.bigdata.DAO.impl

import com.uek.bigdata.DAO.inter.IMonitorDao
import com.uek.bigdata.daomain.{MonitorState, TopNMonitor2CarCount, TopNMonitorDetailInfo}
import com.uek.bigdata.utils.JDBCUtils

import scala.collection.mutable.ListBuffer

/**
  * 类名称: MonitorDaoImpl
  * 类描述:
  *
  * @Time 2018/12/1 22:13
  * @author 武建谦
  */
class MonitorDaoImpl extends IMonitorDao {
  val jdbc = JDBCUtils.getInstance()

  /**
    * 将自定义累加器记录的五种状态插入到数据库：
    *
    * @param monitorState
    */
  override def insertMonitorState(monitorState: MonitorState): Unit = {
    val sql = "insert into monitor_state values(?,?,?,?,?,?)"
    val params = Array[Any](
      monitorState.getTaskId,
      monitorState.getNoramlMonitorCount,
      monitorState.getNormalCameraCount,
      monitorState.getAbnormalMonitorCount,
      monitorState.getAbnormalCameraCount,
      monitorState.getAbnormalMonitorCameraInfos
    )
    val i = jdbc.executorUpdate(sql, params)
    if (i == 1) {
      println("插入成功")
    } else {
      println("插入失败")
    }
  }

  /**
    * 获取车流量topN的卡口
    *
    * @param topns
    */
  override def insertTopNMonitor2CarCount(topns: ListBuffer[TopNMonitor2CarCount]): Unit = {
    val sql = "insert into topn_monitor_car_count values(?,?,?)"
    val params = new ListBuffer[Array[Any]]
    for (obj <- topns) {
      println("插入操作：" + obj)
      val param = Array[Any](obj.getTaskId, obj.getMonitorId, obj.getCount)
      params += (param)
    }
    val is = jdbc.executeBatch(sql, params)
    if (is.length > 0) {
      println("插入成功")
    } else {
      println("插入失败")
    }
  }

  /**
    * 将topN卡口中的详细信息插入到数据库中
    *
    * @param topNMonitorDetailInfos
    */
  override def insertTopNMonitorDetailInfo(topNMonitorDetailInfos: ListBuffer[TopNMonitorDetailInfo]): Unit = {
    val sql = "insert into topn_monitor_detail_info values(?,?,?,?,?,?,?,?)"
    val params = new ListBuffer[Array[Any]]
    for (topNMonitorDetailInfo <- topNMonitorDetailInfos) {
      params +=
        Array[Any](
          topNMonitorDetailInfo.getTaskId,
          topNMonitorDetailInfo.getDate,
          topNMonitorDetailInfo.getMonitorId,
          topNMonitorDetailInfo.getCameraId,
          topNMonitorDetailInfo.getCar,
          topNMonitorDetailInfo.getActionTime,
          topNMonitorDetailInfo.getSpeed,
          topNMonitorDetailInfo.getRoadId
        )
    }
    val num = jdbc.executeBatch(sql,params)
    if (num.length > 0) {
      println("TopNTail,插入成功")
    }else{
      println("TopNTail,插入失败")
    }
  }
}
