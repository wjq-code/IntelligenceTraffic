package com.uek.bigdata.model.udaf;

import org.apache.spark.sql.api.java.UDF1;

/**
 * 类名称: RemoveRandomPrefixUDF
 * 类描述: 删除随机数前缀
 *
 * @author 武建谦
 * @Time 2018/11/29 15:09
 */
public class RemoveRandomPrefixUDF implements UDF1<String,String> {
    @Override
    public String call(String s) throws Exception {
        String[] split = s.split("_");
        return split[1];
    }
}
