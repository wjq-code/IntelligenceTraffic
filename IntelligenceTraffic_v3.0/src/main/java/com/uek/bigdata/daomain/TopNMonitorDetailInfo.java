package com.uek.bigdata.daomain;

/**
 * 类名称: TopNMonitorDetailInfo
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/26 19:23
 */
public class TopNMonitorDetailInfo {
    private String taskId;
    private String date;
    private String monitor_id;
    private String camera_id;
    private String car;
    private String action_time;
    private String speed;
    private String road_id;

    public TopNMonitorDetailInfo() {
    }

    public TopNMonitorDetailInfo(String taskId, String date, String monitor_id, String camera_id, String car, String action_time, String speed, String road_id) {
        this.taskId = taskId;
        this.date = date;
        this.monitor_id = monitor_id;
        this.camera_id = camera_id;
        this.car = car;
        this.action_time = action_time;
        this.speed = speed;
        this.road_id = road_id;
    }

    @Override
    public String toString() {
        return "TopNMonitorDetailInfo{" +
                "task='" + taskId + '\'' +
                ", date='" + date + '\'' +
                ", monitor_id='" + monitor_id + '\'' +
                ", camera_id='" + camera_id + '\'' +
                ", car='" + car + '\'' +
                ", action_time='" + action_time + '\'' +
                ", speed='" + speed + '\'' +
                ", road_id='" + road_id + '\'' +
                '}';
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTask(String taskId) {
        this.taskId = taskId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getMonitor_id() {
        return monitor_id;
    }

    public void setMonitor_id(String monitor_id) {
        this.monitor_id = monitor_id;
    }

    public String getCamera_id() {
        return camera_id;
    }

    public void setCamera_id(String camera_id) {
        this.camera_id = camera_id;
    }

    public String getCar() {
        return car;
    }

    public void setCar(String car) {
        this.car = car;
    }

    public String getAction_time() {
        return action_time;
    }

    public void setAction_time(String action_time) {
        this.action_time = action_time;
    }

    public String getSpeed() {
        return speed;
    }

    public void setSpeed(String speed) {
        this.speed = speed;
    }

    public String getRoad_id() {
        return road_id;
    }

    public void setRoad_id(String road_id) {
        this.road_id = road_id;
    }
}
