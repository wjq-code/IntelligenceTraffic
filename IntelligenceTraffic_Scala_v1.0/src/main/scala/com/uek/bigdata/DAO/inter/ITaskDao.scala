package com.uek.bigdata.DAO.inter

import com.uek.bigdata.daomain.Task

/**
  * 类名称: ITaskDao
  * 类描述:
  *
  * @Time 2018/11/29 1:09
  * @author 武建谦
  */
trait ITaskDao {
  //根据task 主键查询制定的任务数据
  def selectTaskById(taskId:Long): Task
}
