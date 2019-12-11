package com.uek.bigdata.DAO.impl;

import com.uek.bigdata.DAO.factory.DAOFactory;
import com.uek.bigdata.DAO.inter.ITaskDao;
import com.uek.bigdata.daomain.Task;
import com.uek.bigdata.utils.JDBCUtils;
import com.uek.bigdata.utils.JDBCUtils.QueryCallBack;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 类名称：TaskDaoImpl
 * 类描述：ITaskDao实现类
 * @author 武建谦
 * @Time 2018/11/25 14:18
 */
public class TaskDaoImpl implements ITaskDao {
    @Override
    public Task selectTaskById(long taskId) {
        String sql = "select * from  task where task_id = ?";
        Object[] param = {taskId};
        JDBCUtils conn = JDBCUtils.getInstance();
        Task task = new Task();
        conn.executorQurey(sql,param,new QueryCallBack(){
            @Override
            public void process(ResultSet rs) {
                try{
                    while (rs.next()){
                        Long taskId = rs.getLong(1);
                        String taskName = rs.getString(2);
                        String createTime = rs.getString(3);
                        String startTime = rs.getString(4);
                        String finishTime = rs.getString(5);
                        String taskType = rs.getString(6);
                        String taskStatus = rs.getString(7);
                        String taskParams = rs.getString(8);
                        task.set(taskId, taskName, createTime, startTime, finishTime, taskType, taskStatus, taskParams);
                    }
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        });
        return task;
    }
}
