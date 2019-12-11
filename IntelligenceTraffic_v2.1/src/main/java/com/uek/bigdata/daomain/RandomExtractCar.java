package com.uek.bigdata.daomain;

/**
 * 类名称: RandomExtractCar
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/28 14:58
 */
public class RandomExtractCar {
    private long taskId;
    private String carNum;
    private String date;
    private String dateHour;

    public RandomExtractCar() {
    }

    public RandomExtractCar(long taskId, String carNum, String date, String dateHour) {
        this.taskId = taskId;
        this.carNum = carNum;
        this.date = date;
        this.dateHour = dateHour;
    }

    @Override
    public String toString() {
        return "RandomExtractCar{" +
                "taskId=" + taskId +
                ", carNum=" + carNum +
                ", date=" + date +
                ", dateHour=" + dateHour +
                '}';
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getCarNum() {
        return carNum;
    }

    public void setCarNum(String carNum) {
        this.carNum = carNum;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getDateHour() {
        return dateHour;
    }

    public void setDateHour(String dateHour) {
        this.dateHour = dateHour;
    }
}
