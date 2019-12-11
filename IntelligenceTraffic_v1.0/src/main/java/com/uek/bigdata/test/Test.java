package com.uek.bigdata.test;

import com.uek.bigdata.utils.JDBCUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 类名称：${class}
 * 类描述：
 *
 * @author 武建谦
 * @Time 2018/11/23 17:42
 */
public class Test {
    public static void main(String[] args) throws SQLException {
        JDBCUtils jdbc = JDBCUtils.getInstance();
        String sql = "select ?,? from t_word";
        Object[] obj = {"num","name"};
        ResultSet rs = jdbc.executorQurey(sql, obj);
        while (rs.next()){
            int id = rs.getInt(1);
            String name = rs.getString("name");
            System.out.println("id:   " + id + "  name:  " + name);
        }

        /*String sql1 = "insert into t_word values (?,?)";
        Object[] obj1 = {1,"22"};
        int i = jdbc.executorUpdate(sql1, obj1);
        System.out.println(i);*/
    }
}
