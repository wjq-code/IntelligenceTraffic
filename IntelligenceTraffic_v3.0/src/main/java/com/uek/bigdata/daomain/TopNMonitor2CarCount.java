package com.uek.bigdata.daomain;

/**
 * 类名称: TopNMonitor2CarCount
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/26 17:59
 */
public class TopNMonitor2CarCount {
    private long taskId;
    private String monitorId;
    private Integer carCount;

    public TopNMonitor2CarCount() {
    }

    public TopNMonitor2CarCount(long taskId, String monitorId, Integer carCount) {
        this.taskId = taskId;
        this.monitorId = monitorId;
        this.carCount = carCount;
    }

    @Override
    public String toString() {
        return "TopNMonitor2CarCount{" +
                "taskId=" + taskId +
                ", monitorId='" + monitorId + '\'' +
                ", carCount='" + carCount + '\'' +
                '}';
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getMonitorId() {
        return monitorId;
    }

    public void setMonitorId(String monitorId) {
        this.monitorId = monitorId;
    }

    public Integer getCarCount() {
        return carCount;
    }

    public void setCarCount(Integer carCount) {
        this.carCount = carCount;
    }
}
