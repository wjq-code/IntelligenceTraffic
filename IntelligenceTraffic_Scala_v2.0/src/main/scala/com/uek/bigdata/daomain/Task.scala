package com.uek.bigdata.daomain
import scala.beans._
/**
  * 类名称: Task
  * 类描述:
  *
  * @Time 2018/11/29 0:59
  * @author 武建谦
  */
class Task extends Serializable {

  //一般在编写JavaBean中，数据库中的字段和实体类的属性名， 以及顺序最好一致
  @BeanProperty var taskId:Long = 0L //任务id
  @BeanProperty var taskName:String = null //任务名称
  @BeanProperty var createTime:String = null //任务创建时间
  @BeanProperty var startTime:String = null //任务开始时间
  @BeanProperty var finishTime:String = null //任务完成时间
  @BeanProperty var taskType:String = null //任务类型
  @BeanProperty var taskStatus:String = null //任务状态
  @BeanProperty var taskParams:String = null //任务的参数（包含有多个参数，以json格式的字符串的形式表示）

  def this(taskId:Long,taskName:String,createTime:String,startTime:String,finishTime:String,taskType:String,taskStatus:String,taskParams:String){
    this()
    this.taskId = taskId
    this.taskName = taskName
    this.createTime = createTime
    this.startTime = startTime
    this.finishTime = finishTime
    this.taskType = taskType
    this.taskStatus = taskStatus
    this.taskParams = taskParams
  }

  override def toString = s"Task($taskId, $taskName, $createTime, $startTime, $finishTime, $taskType, $taskStatus, $taskParams)"
}
