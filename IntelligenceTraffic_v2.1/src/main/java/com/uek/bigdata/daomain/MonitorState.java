package com.uek.bigdata.daomain;

/**
 * 类名称: MonitorState
 * 类描述: 卡口状态javaBean
 *
 * @author 武建谦
 * @Time 2018/11/26 16:36
 */
public class MonitorState {
    private long taskId;
    private String normalMonitorCount;
    private String normalCameraCount;
    private String abnormalMonitorCount;
    private String abnormalCameraCount;
    private String abnormalMonitorCameraInfos;

    public MonitorState() {
    }

    public MonitorState(long taskId, String normalMonitorCount, String normalCameraCount, String abnormalMonitorCount, String abnormalCameraCount, String abnormalMonitorCameraInfos) {
        this.taskId = taskId;
        this.normalMonitorCount = normalMonitorCount;
        this.normalCameraCount = normalCameraCount;
        this.abnormalMonitorCount = abnormalMonitorCount;
        this.abnormalCameraCount = abnormalCameraCount;
        this.abnormalMonitorCameraInfos = abnormalMonitorCameraInfos;
    }

    @Override
    public String toString() {
        return "MonitorState{" +
                "taskId=" + taskId +
                ", normalMonitorCount='" + normalMonitorCount + '\'' +
                ", normalCameraCount='" + normalCameraCount + '\'' +
                ", abnormalMonitorCount='" + abnormalMonitorCount + '\'' +
                ", abnormalCameraCount='" + abnormalCameraCount + '\'' +
                ", abnormalMonitorCameraInfos='" + abnormalMonitorCameraInfos + '\'' +
                '}';
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getNormalMonitorCount() {
        return normalMonitorCount;
    }

    public void setNormalMonitorCount(String normalMonitorCount) {
        this.normalMonitorCount = normalMonitorCount;
    }

    public String getNormalCameraCount() {
        return normalCameraCount;
    }

    public void setNormalCameraCount(String normalCameraCount) {
        this.normalCameraCount = normalCameraCount;
    }

    public String getAbnormalMonitorCount() {
        return abnormalMonitorCount;
    }

    public void setAbnormalMonitorCount(String abnormalMonitorCount) {
        this.abnormalMonitorCount = abnormalMonitorCount;
    }

    public String getAbnormalCameraCount() {
        return abnormalCameraCount;
    }

    public void setAbnormalCameraCount(String abnormalCameraCount) {
        this.abnormalCameraCount = abnormalCameraCount;
    }

    public String getAbnormalMonitorCameraInfos() {
        return abnormalMonitorCameraInfos;
    }

    public void setAbnormalMonitorCameraInfos(String abnormalMonitorCameraInfos) {
        this.abnormalMonitorCameraInfos = abnormalMonitorCameraInfos;
    }
}
