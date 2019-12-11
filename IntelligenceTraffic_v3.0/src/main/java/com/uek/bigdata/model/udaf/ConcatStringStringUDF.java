package com.uek.bigdata.model.udaf;

import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;

/**
 * 类名称: ConcatStringStringUDF
 * 类描述: 将两个字段拼接起来
 *
 * @author 武建谦
 * @Time 2018/11/29 15:05
 */
public class ConcatStringStringUDF implements UDF3<String,String,String,String> {
    @Override
    public String call(String s, String s2, String regex) throws Exception {
        return s + regex + s2;
    }
}
