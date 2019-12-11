package com.uek.bigdata.daomain;

import java.io.Serializable;

/**
 * 类名称：${class}
 * 类描述：
 *        任务实体类
 * 注意： 在创建task数据库标时，添加的第一行数据中的task_params字段中的日期必须是当前日期，否则查询不到（模拟数据就是当前时期）
 * 为什么字段数据要使用json字符串表示
 *  因为使用json格式的数据可以灵活的代替多个属性值，这样就不需要在创建表时设置很多字段，统一采用json格式字符串来代替
 *  task表的创建时间，结束时间等 字段，是给平台使用者的一种提示性信息，比如平台使用者提交某个查询是什么时间进行的，什么时间结束的
 *  对于当前业务来说，只需要使用到task表中的参数字段
 *  该task就是数据库中task表中的Javabean
 * @author 武建谦
 * @Time 2018/11/25 11:51
 */
public class Task implements Serializable {
    //一般在编写JavaBean中，数据库中的字段和实体类的属性名， 以及顺序最好一致
    private Long taskId;                           //任务id
    private String taskName;                      //任务名称
    private String createTime;                    //任务创建时间
    private String startTime;                     //任务开始时间
    private String finishTime;                    //任务完成时间
    private String taskType;                      //任务类型
    private String taskStatus;                    //任务状态
    private String taskParams;                    //任务的参数（包含有多个参数，以json格式的字符串的形式表示）

    public Task() {
    }

    public Task(Long taskId, String taskName, String createTime, String startTime, String finishTime, String taskType, String taskStatus, String taskParams) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.createTime = createTime;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.taskType = taskType;
        this.taskStatus = taskStatus;
        this.taskParams = taskParams;
    }

    public void set(Long taskId, String taskName, String createTime, String startTime, String finishTime, String taskType, String taskStatus, String taskParams) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.createTime = createTime;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.taskType = taskType;
        this.taskStatus = taskStatus;
        this.taskParams = taskParams;
    }

    @Override
    public String toString() {
        return "Task{" +
                "taskId=" + taskId +
                ", taskName='" + taskName + '\'' +
                ", createTime='" + createTime + '\'' +
                ", startTime='" + startTime + '\'' +
                ", finishTime='" + finishTime + '\'' +
                ", taskType='" + taskType + '\'' +
                ", taskStatus='" + taskStatus + '\'' +
                ", taskParams='" + taskParams + '\'' +
                '}';
    }

    public Long getTaskId() {
        return taskId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
    }

    public String getTaskParams() {
        return taskParams;
    }

    public void setTaskParams(String taskParams) {
        this.taskParams = taskParams;
    }
}
