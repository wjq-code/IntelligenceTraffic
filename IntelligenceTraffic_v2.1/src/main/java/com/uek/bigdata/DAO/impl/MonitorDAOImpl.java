package com.uek.bigdata.DAO.impl;

import com.uek.bigdata.DAO.inter.IMonitorDAO;
import com.uek.bigdata.daomain.MonitorState;
import com.uek.bigdata.daomain.TopNMonitor2CarCount;
import com.uek.bigdata.daomain.TopNMonitorDetailInfo;
import com.uek.bigdata.utils.JDBCUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 类名称: MonitorDAOImpl
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/26 16:40
 */
public class MonitorDAOImpl implements IMonitorDAO {
    JDBCUtils jdbc = JDBCUtils.getInstance();

    @Override
    public void insertMonitorState(MonitorState monitorState) {
        String sql = "insert into monitor_state values(?,?,?,?,?,?)";
        Object[] param = {
                monitorState.getTaskId() + "",
                monitorState.getNormalMonitorCount(),
                monitorState.getNormalCameraCount(),
                monitorState.getAbnormalMonitorCount(),
                monitorState.getAbnormalCameraCount(),
                monitorState.getAbnormalMonitorCameraInfos()
        };
        List<Object[]> params = new ArrayList<>();
        params.add(param);
        int[] ints = jdbc.executeBatch(sql, params);
        if (ints.length >= 1) {
            System.out.println("批量插入成功");
        } else {
            System.out.println("批量插入失败");
        }
    }

    @Override
    public void insertBatchTopN(List<TopNMonitor2CarCount> topNMonitor2CarCounts) {
        String sql = "insert into topn_monitor_car_count values(?,?,?)";
        List params = new ArrayList<>();
        for (TopNMonitor2CarCount topNMonitor2CarCount : topNMonitor2CarCounts) {
            Object[] param = {
                    topNMonitor2CarCount.getTaskId() + "",
                    topNMonitor2CarCount.getMonitorId(),
                    topNMonitor2CarCount.getCarCount()
            };
            params.add(param);
        }
        int[] ints = jdbc.executeBatch(sql, params);
        if (ints.length >= 1) {
            System.out.println("批量插入成功");
        } else {
            System.out.println("批量插入失败");
        }
    }

    @Override
    public void insertBatchMonitorDetails(List<TopNMonitorDetailInfo> monitorDetailInfos) {
        String sql = "insert into topn_monitor_detail_info values(?,?,?,?,?,?,?,?)";
        List params = new ArrayList<>();
        for (TopNMonitorDetailInfo topNMonitorDetailInfo : monitorDetailInfos) {
            Object[] param = {
                    topNMonitorDetailInfo.getTaskId(),
                    topNMonitorDetailInfo.getDate(),
                    topNMonitorDetailInfo.getMonitor_id(),
                    topNMonitorDetailInfo.getCamera_id(),
                    topNMonitorDetailInfo.getCar(),
                    topNMonitorDetailInfo.getAction_time(),
                    topNMonitorDetailInfo.getSpeed(),
                    topNMonitorDetailInfo.getRoad_id()
            };
            params.add(param);
        }
        int[] ints = jdbc.executeBatch(sql, params);
        if (ints.length >= 1) {
            System.out.println("批量插入成功");
        } else {
            System.out.println("批量插入失败");
        }
    }

    @Override
    public void insertBatchTop10Details(List<TopNMonitorDetailInfo> topNMonitorDetailInfos) {
        String sql = "insert into top10_speed_detail values(?,?,?,?,?,?,?,?)";
        List<Object[]> params = new ArrayList<>();
        for (TopNMonitorDetailInfo topNMonitorDetailInfo : topNMonitorDetailInfos) {
            params.add(
                    new Object[]{
                            topNMonitorDetailInfo.getTaskId(),
                            topNMonitorDetailInfo.getDate(),
                            topNMonitorDetailInfo.getMonitor_id(),
                            topNMonitorDetailInfo.getCamera_id(),
                            topNMonitorDetailInfo.getCar(),
                            topNMonitorDetailInfo.getAction_time(),
                            topNMonitorDetailInfo.getSpeed(),
                            topNMonitorDetailInfo.getRoad_id()
                    }
            );
        }
        int[] ints = jdbc.executeBatch(sql, params);
        if (ints.length >= 1) {
            System.out.println("批量插入成功");
        } else {
            System.out.println("批量插入失败");
        }
    }
}
