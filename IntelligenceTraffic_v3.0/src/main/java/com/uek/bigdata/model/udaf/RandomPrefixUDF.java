package com.uek.bigdata.model.udaf;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * 类名称: RandomPrefixUDF
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/29 15:07
 */
public class RandomPrefixUDF implements UDF2<String,Integer,String> {
    @Override
    public String call(String s, Integer random) throws Exception {
        Random random1 = new Random();
        int prefix = random1.nextInt(random);
        return prefix + "_" + s;
    }
}
