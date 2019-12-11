package com.uek.bigdata.DAO.impl

import java.sql.ResultSet

import com.uek.bigdata.DAO.inter.ITaskDao
import com.uek.bigdata.daomain.Task
import com.uek.bigdata.utils.JDBCUtils
import com.uek.bigdata.utils.JDBCUtils.QueryCallBack

/**
  * 类名称: TaskDaoImpl
  * 类描述:
  *
  * @Time 2018/11/29 1:09
  * @author 武建谦
  */
class TaskDaoImpl extends ITaskDao{
  val jdbc = JDBCUtils.getInstance()
  override def selectTaskById(taskId:Long): Task = {
    val sql = "select * from task where task_id = ?"
    val param = Array[Any](taskId)
    var task: Task = null;
    jdbc.executorQurey(sql,param,new QueryCallBack() {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          val taskId = rs.getLong(1)
          val taskName = rs.getString(2)
          val createTime = rs.getString(3)
          val startTime = rs.getString(4)
          val finishTime = rs.getString(5)
          val taskType = rs.getString(6)
          val taskStatus = rs.getString(7)
          val taskParams = rs.getString(8)
          task = new Task(taskId,taskName,createTime,startTime,finishTime,taskType,taskStatus,taskParams)
        }
      }
    })
    task
  }
}