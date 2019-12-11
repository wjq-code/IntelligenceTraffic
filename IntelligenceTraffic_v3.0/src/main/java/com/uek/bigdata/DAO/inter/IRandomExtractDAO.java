package com.uek.bigdata.DAO.inter;

import com.uek.bigdata.daomain.CarTrack;
import com.uek.bigdata.daomain.RandomExtractCar;
import com.uek.bigdata.daomain.RandomExtractMonitorDetail;

import java.util.List;

/**
 * 类名称: IRandomExtractDAO
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/28 15:20
 */
public interface IRandomExtractDAO {
    /**
     * 向数据库中插入随机抽取的车辆信息
     * @param carRandomExtracts
     */
    void insertBatchRandomExtractCar(List<RandomExtractCar> carRandomExtracts);

    void insertBatchRandomExtractDetails(List<RandomExtractMonitorDetail> randomExtractMonitorDetails);

    void insertBatchCarTrack(List<CarTrack> carTracks);
}
