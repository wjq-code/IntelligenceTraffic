package com.uek.bigdata.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uek.bigdata.config.ConfigurationManager;
import com.uek.bigdata.constants.Constants;

/**
 * 类名称：ParamsUtils
 * 类描述：参数处理工具类
 * @author 武建谦
 * @Time 2018/11/25 14:48
 */
public class ParamsUtils {

    /**
     * Json格式字符串操作API
     * "[{'学生':'张三', '班级':'一班', '年级':'大一', '科目':'高数', '成绩':90}, {'学生':'李四', '班级':'二班', '年级':'大一', '科目':'高数', '成绩':80}]";
     */
/*    public static void main(String[] args){
        String jsonStr = "[{'学生':'张三', '班级':'一班', '年级':'大一', '科目':'高数', '成绩':90}, {'学生':'李四', '班级':'二班', '年级':'大一', '科目':'高数', '成绩':80}]";
        //1、构建json数组,把json字符串中的数据进行解析成多个元素（json对象）
        JSONArray jsonArray = JSONArray.parseArray(jsonStr);
        //通过索引来获取json对象
        JSONObject jsonObject = jsonArray.getJSONObject(0);
        //通过Key来过去value
    }*/

    /**
     * 功能：从命令行参数中提取任务id
     * @param args 命令行参数
     * @param taskType 任务类型
     * @return 任务id
     */
    public static Long getTaskIdByArgs(String[] args ,String taskType){
        //判断时候为本地运行模式
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if (local) {
            return ConfigurationManager.getLong(taskType);
        }else {
            if (args != null && args.length > 0) {
                return Long.valueOf(args[0]);
            }
        }
        return null;
    }

    /**
     * 功能：从Json对象中提取参数
     * @param jsonObject json对象
     * @param fieldName 参数名
     * @return 参数值
     */
    public static String getParamByJsonStr(JSONObject jsonObject ,String fieldName){
        JSONArray jsonArray = jsonObject.getJSONArray(fieldName);
        if(jsonArray != null && jsonArray.size() > 0) {
            return jsonArray.getString(0);
        }
        return null;
    }
}
