package com.uek.bigdata.DAO.inter;

import com.uek.bigdata.daomain.Task;

/**
 * 类名称：ITaskDao
 * 类描述：任务管理dao层接口
 * @author 武建谦
 * @Time 2018/11/25 14:16
 */
public interface ITaskDao {
    //根据task 主键查询制定的任务数据
    public Task selectTaskById(long taskId);
}
