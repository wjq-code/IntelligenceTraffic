package com.uek.bigdata.DAO.impl;

import com.uek.bigdata.DAO.inter.IRandomExtractDAO;
import com.uek.bigdata.daomain.CarTrack;
import com.uek.bigdata.daomain.RandomExtractCar;
import com.uek.bigdata.daomain.RandomExtractMonitorDetail;
import com.uek.bigdata.utils.JDBCUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 类名称: RandomExtractDAOImpl
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/28 15:20
 */
public class RandomExtractDAOImpl implements IRandomExtractDAO {
    JDBCUtils jdbc = JDBCUtils.getInstance();
    @Override
    public void insertBatchRandomExtractCar(List<RandomExtractCar> carRandomExtracts) {
        String sql = "insert into random_extract_car values(?,?,?,?)";
        List<Object[]> params = new ArrayList<>();
        for (RandomExtractCar carRandomExtract : carRandomExtracts) {
            params.add(
                    new Object[]{
                            carRandomExtract.getTaskId(),
                            carRandomExtract.getCarNum(),
                            carRandomExtract.getDate(),
                            carRandomExtract.getDateHour()
                    }
            );
        }
        int[] ints = jdbc.executeBatch(sql, params);
        if (ints.length >= 1) {
            System.out.println("批量插入成功");
        }else {
            System.out.println("批量插入成功");
        }
    }

    @Override
    public void insertBatchRandomExtractDetails(List<RandomExtractMonitorDetail> randomExtractMonitorDetails) {
        String sql = "insert into random_extract_car_detail_info values(?,?,?,?,?,?,?,?)";
        List<Object[]> params = new ArrayList<>();
        for (RandomExtractMonitorDetail randomExtractMonitorDetail : randomExtractMonitorDetails) {
            params.add(
                    new Object[]{
                            randomExtractMonitorDetail.getTaskId(),
                            randomExtractMonitorDetail.getDate(),
                            randomExtractMonitorDetail.getMonitor_id(),
                            randomExtractMonitorDetail.getCamera_id(),
                            randomExtractMonitorDetail.getCar(),
                            randomExtractMonitorDetail.getAction_time(),
                            randomExtractMonitorDetail.getSpeed(),
                            randomExtractMonitorDetail.getRoad_id()
                    }
            );
        }
        int[] ints = jdbc.executeBatch(sql, params);
        if (ints.length >= 1) {
            System.out.println("批量插入成功");
        }else {
            System.out.println("批量插入成功");
        }
    }

    @Override
    public void insertBatchCarTrack(List<CarTrack> carTracks) {
        String sql = "insert into car_track values(?,?,?,?)";
        List<Object[]> params = new ArrayList<>();
        for (CarTrack carTrack : carTracks) {
            params.add(
                    new Object[]{
                            carTrack.getTaskId(),
                            carTrack.getDate(),
                            carTrack.getCarNum(),
                            carTrack.getTrack()
                    }
            );
        }
        int[] ints = jdbc.executeBatch(sql, params);
        if (ints.length >= 1) {
            System.out.println("car批量插入成功");
        }else {
            System.out.println("car批量插入成功");
        }
    }
}
