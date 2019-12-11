package com.uek.bigdata.DAO.inter;

import com.uek.bigdata.daomain.MonitorState;
import com.uek.bigdata.daomain.TopNMonitor2CarCount;
import com.uek.bigdata.daomain.TopNMonitorDetailInfo;

import java.util.List;

/**
 * 类名称: IMonitorDAO
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/26 16:43
 */
public interface IMonitorDAO {
    /**
     * 将自定义累加器记录的五种状态插入到数据库：
     * @param monitorState
     */
    void insertMonitorState(MonitorState monitorState);

    /**
     * 将车流量最大的topN卡口信息插入到数据库：
     * @param topNMonitor2CarCounts
     */
    void insertBatchTopN(List<TopNMonitor2CarCount> topNMonitor2CarCounts);

    /**
     *将车流量最大的topN卡口的详细信息插入到数据库：
     */
    void insertBatchMonitorDetails(List<TopNMonitorDetailInfo> topNMonitorDetailInfo);
    /**
     * 被高速通过的前5个卡口，每个卡口下的最快的10辆车的信息插入到数据库：
     * @param topNMonitorDetailInfos
     */
    void insertBatchTop10Details(List<TopNMonitorDetailInfo> topNMonitorDetailInfos);
}
