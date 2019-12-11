package com.uek.bigdata.daomain;

import java.io.Serializable;

/**
 * 类名称: SpeedSortKey
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/27 13:50
 */
public class SpeedSortKey implements Serializable ,Comparable<SpeedSortKey> {
    private long lowSpeed;       //低速车辆
    private long normalSpeed;    //正常车速车辆
    private long mediumSpeed;    //中速车速车辆
    private long highSpeed;      //高速车速车辆

    public SpeedSortKey() {
    }

    public SpeedSortKey(long lowSpeed, long normalSpeed, long mediumSpeed, long highSpeed) {
        this.lowSpeed = lowSpeed;
        this.normalSpeed = normalSpeed;
        this.mediumSpeed = mediumSpeed;
        this.highSpeed = highSpeed;
    }

    @Override
    public String toString() {
        return "SpeedSortKey{" +
                "lowSpeed=" + lowSpeed +
                ", normalSpeed=" + normalSpeed +
                ", mediumSpeed=" + mediumSpeed +
                ", highSpeed=" + highSpeed +
                '}';
    }

    public long getLowSpeed() {
        return lowSpeed;
    }

    public void setLowSpeed(long lowSpeed) {
        this.lowSpeed = lowSpeed;
    }

    public long getNormalSpeed() {
        return normalSpeed;
    }

    public void setNormalSpeed(long normalSpeed) {
        this.normalSpeed = normalSpeed;
    }

    public long getMediumSpeed() {
        return mediumSpeed;
    }

    public void setMediumSpeed(long mediumSpeed) {
        this.mediumSpeed = mediumSpeed;
    }

    public long getHighSpeed() {
        return highSpeed;
    }

    public void setHighSpeed(long highSpeed) {
        this.highSpeed = highSpeed;
    }

    @Override
    public int compareTo(SpeedSortKey o) {
        if (this.highSpeed == o.highSpeed) {
            if (this.mediumSpeed == o.mediumSpeed) {
                if (this.normalSpeed == o.normalSpeed) {
                    return  Long.compare(this.lowSpeed,o.lowSpeed);
                }
                return Long.compare(this.normalSpeed,o.normalSpeed);
            }
            return Long.compare(this.mediumSpeed,o.mediumSpeed);
        }
        return Long.compare(this.highSpeed,o.highSpeed);
    }
}
