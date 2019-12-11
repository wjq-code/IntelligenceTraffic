package com.uek.bigdata.DAO.factory

import com.uek.bigdata.DAO.impl.{MonitorDaoImpl, TaskDaoImpl}
import com.uek.bigdata.DAO.inter.{IMonitorDao, ITaskDao}

/**
  * 类名称: DAOFactory
  * 类描述:
  *
  * @Time 2018/11/29 1:16
  * @author 武建谦
  */
object DAOFactory {
  def getTaskDao():ITaskDao ={
    new TaskDaoImpl
  }
  def getMonitor():IMonitorDao={
    new MonitorDaoImpl
  }
}
