package com.uek.bigdata.DAO.factory;

import com.uek.bigdata.DAO.impl.MonitorDAOImpl;
import com.uek.bigdata.DAO.impl.RandomExtractDAOImpl;
import com.uek.bigdata.DAO.impl.TaskDaoImpl;
import com.uek.bigdata.DAO.inter.IMonitorDAO;
import com.uek.bigdata.DAO.inter.IRandomExtractDAO;
import com.uek.bigdata.DAO.inter.ITaskDao;
import com.uek.bigdata.daomain.MonitorState;

/**
 * 类名称：DAOFactory
 * 类描述：DAO层管理类（工厂模式）
 *
 * @author 武建谦
 * @Time 2018/11/25 14:29
 */
public class DAOFactory {
    //对外提供任务接口是实现类
    public static ITaskDao getTaskDao() {
        return new TaskDaoImpl();
    }

    //
    public static IMonitorDAO getMonitorDAO() {return new MonitorDAOImpl();}

    public static IRandomExtractDAO getRandomExtractDAO() {return new RandomExtractDAOImpl();}

}
