package com.uek.bigdata.daomain;

/**
 * 类名称: CarTrack
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/28 17:14
 */
public class CarTrack {
    private long taskId;
    private String date;
    private String carNum;
    private String track;

    public CarTrack() {
    }

    public CarTrack(long taskId, String date, String carNum, String track) {
        this.taskId = taskId;
        this.date = date;
        this.carNum = carNum;
        this.track = track;
    }

    @Override
    public String toString() {
        return "CarTrack{" +
                "taskId=" + taskId +
                ", date='" + date + '\'' +
                ", carNum='" + carNum + '\'' +
                ", track='" + track + '\'' +
                '}';
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCarNum() {
        return carNum;
    }

    public void setCarNum(String carNum) {
        this.carNum = carNum;
    }

    public String getTrack() {
        return track;
    }

    public void setTrack(String track) {
        this.track = track;
    }
}
