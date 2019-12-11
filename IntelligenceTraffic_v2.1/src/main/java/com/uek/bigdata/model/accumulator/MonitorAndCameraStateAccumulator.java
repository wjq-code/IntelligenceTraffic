package com.uek.bigdata.model.accumulator;

import com.uek.bigdata.constants.Constants;
import com.uek.bigdata.utils.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * 类名称：MonitorAndCameraStateAccumulator
 * 类描述：
 *
 * @author 武建谦
 * @Time 2018/11/25 18:18
 */
public class MonitorAndCameraStateAccumulator implements AccumulatorParam<String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    @Override
    public String zero(String initialValue) {
        String statusStr =
                Constants.FIELD_NORMAL_MONITOR_COUNT + "=0|"            //正常卡口
                        + Constants.FIELD_NORMAL_CAMERA_COUNT + "=0|"
                        + Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=0|"
                        + Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=0|"
                        + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "= ";
        return statusStr;
    }

    /**
     * @param v1 连接串，上次累加后的结果
     * @param v2 本次累加传入的值
     * @return 更新后的连接串
     */
    private String add(String v1, String v2) {
        //第一次在传入的时候回直接返回
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }
        //对传入的字符串进行切割
        String[] kvStrs = v2.split("\\|");
        for (String kvStr : kvStrs) {
            String[] kvs = kvStr.split("=", 2);
            String countName = kvs[0];
            String countValue = kvs[1];

            if (countName.equals(Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS)) {
                String oldValue = StringUtils.extractValue(v1, "|", countName);
                v1 = StringUtils.setFieldValue(v1, "|", countName, oldValue + "-" + countValue);
            } else {
                //获取相对应上面的countName
                String oldValue = StringUtils.extractValue(v1, "|", countName);
                Integer newValue = Integer.valueOf(oldValue) + Integer.valueOf(countValue);
                v1 = StringUtils.setFieldValue(v1, "|", countName, String.valueOf(newValue));
            }
        }
        return v1;
    }
}
