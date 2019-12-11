package com.uek.bigdata.model.udaf;

import com.uek.bigdata.utils.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Map;

/**
 * 类名称: GroupConcatDistinctUDAF
 * 类描述:
 *
 * @author 武建谦
 * @Time 2018/11/29 12:37
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {
    //定义输入数据的字段和类型
    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("carInfo", DataTypes.StringType, true)));
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("bufferInfo", DataTypes.StringType, true)));

    //指定返回值类型
    private DataType dataType = DataTypes.StringType;
    // 指定是否是确定性的
    private boolean deterministic = true;

    //输入的数据类型
    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    //指定缓冲的数据类型
    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    //指定是否正确性
    @Override
    public boolean deterministic() {
        return deterministic;
    }

    //初始化操作，0索引，空串
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    /*
     * 5、更新操作：
     * 可以认为是，一个一个地将组内的字段值传递进来，实现拼接的逻辑，相当于map 端的小聚合 combiner
     * buffer：上一次聚合的结果
     * input：当前传入的值 monitor_id
     * 数据格式：monitor_id1=10|monitor_id2=8
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        //上次的字符串
        String bufferMonitorInfo = buffer.getString(0);
        //新传入的字符串
        String inputMonitorInfo = input.getString(0);
        //解决数据倾斜
        String[] split = inputMonitorInfo.split("\\|");
        String monitorId = "";
        int addNum = 1;
        for (String _monitorId : split) {
            if (_monitorId.indexOf("=") != -1) {
                monitorId = _monitorId.split("=")[0];
                addNum = Integer.valueOf(_monitorId.split("=")[1]);
            } else {
                monitorId = _monitorId;
            }
            //获取上次的统计结果
            String oldValue = StringUtils.extractValue(bufferMonitorInfo, "|", monitorId);
            if (oldValue == null) {
                bufferMonitorInfo += "|" + monitorId + "=" + addNum;
            } else {
                //更新上次的旧值
                bufferMonitorInfo = StringUtils.setFieldValue(bufferMonitorInfo, "|", monitorId, String.valueOf(Integer.valueOf(oldValue) + addNum));
            }
            buffer.update(0, bufferMonitorInfo);
        }
    }

    /*
     * 6、大合并操作：
     * update操作，可能是针对一个分组内的部分数据，在某个节点上发生的
     * 但是可能一个分组内的数据，会分布在多个节点上处理
     * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        //缓存中的大字符串
        String bufferMonitorInfo1 = buffer1.getString(0);
        //新传入的字符串
        String bufferMonitorInfo2 = buffer2.getString(0).substring(1);
        //把buffer2中的数据拆出来更新
        String[] split = bufferMonitorInfo2.split("\\|");
        for (String monitorInfo : split) {
            Map<String, String> map = StringUtils.getMap(monitorInfo, "|");
            for (Map.Entry<String, String> entry : map.entrySet()) {
                //卡口id
                String monitorId = entry.getKey();
                //卡口下的车辆数
                Integer count = Integer.valueOf(entry.getValue());

                String oldValue = StringUtils.extractValue(bufferMonitorInfo1, "|", monitorId);

                if (oldValue == null) {
                    if ("".equals(bufferMonitorInfo1)) {
                        bufferMonitorInfo1 += monitorId + "=" + count;
                    } else {
                        bufferMonitorInfo1 += "|" + monitorId + "=" + count;
                    }
                } else {
                    bufferMonitorInfo1 = StringUtils.setFieldValue(bufferMonitorInfo1, "|", monitorId, String.valueOf(Integer.valueOf(oldValue) + count));
                }
                buffer1.update(0, bufferMonitorInfo1);
            }
        }
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
