package com.uek.bigdata.utils;

import scala.annotation.meta.field;

import java.util.HashMap;
import java.util.Map;

/**
 * 需求：
 *
 * @author 武建谦
 * @Time 2018/11/23 10:32
 */
public class StringUtils {
    /**
     * @param str 输入一个字符串类型的形参
     * @return 然后判断形参是否为空
     */
    public static boolean isEmpty(String str) {
        return "".equals(str.trim()) || null == str;
    }

    public static boolean isNotEmpty(String str) {
        return !"".equals(str.trim()) || null != str;
    }

    /**
     * 对传入的形参进行补全n位数字
     *
     * @param num 指定补全的位数
     * @param str 要补全的字符串
     * @return
     */
    public static String complement(int num, String str) {
        //构建可变长的字符串对象
        StringBuilder sb = new StringBuilder();
        //判断字符串的长度和要求的长度是否一样
        if (str.length() == num) {
            return str;
        } else {
            //确定要补全的位数
            int n = num - str.length();
            for (int i = 0; i < n; i++) {
                sb.append(0);
            }
            //拼接
            sb.append(str);
            return sb.toString();
        }
    }

    /**
     * @param str   传入的字符串
     * @param regex 指定的字符串分隔符
     * @param field 指定字段名
     * @return 从传入的形参中抽取指定的字段的value值
     * name=zhangshengxiao|age=25|sex=man|hunfu=
     */
    public static String extractValue(String str, String regex, String field) {
        String[] line = str.split("\\" + regex);
        for (String fields : line) {
            String[] split = fields.split("=");
            if (split.length == 2) {
                String fieldName = split[0];
                String fieldValue = split[1];
                if (field.equals(fieldName)) {
                    return fieldValue;
                }
            }
        }
        return null;
    }


    /**
     * 给指定的字段设置值，
     *
     * @param str       传入的字符串
     * @param regex     指定分隔符
     * @param fieldName 指定的字段名
     * @param fieldVale 字段名对应的值
     * @return 返回一个更新后的字符串
     */
    public static String setFieldValue(String str, String regex, String fieldName, String fieldVale) {
        String[] line = str.split("\\" + regex);
        StringBuilder sb = new StringBuilder();
        for (String fields : line) {
            String[] split = fields.split("=");
            if (split[0].equals(fieldName)) {
                sb.append(fieldName).append("=").append(fieldVale).append("|");
            } else {
                sb.append(fields).append("|");
            }
        }
        return sb.toString().substring(0, sb.toString().length() - 1);
    }

    /**
     * @param str 传入的字符串
     * @return 字符串对应的integer类型
     */
    public static Integer str2Integer(String str) {
        return Integer.valueOf(str);
    }

    /**
     *
     * @param str 出入字符串
     * @return 截断字符串两侧的，
     */
    public static String trimStr(String str){
        //判断字符串前面有逗号
        if (str.startsWith(",")) {
            str = str.substring(1);
        }
        if (str.endsWith(",")) {
            str = str.substring(0, str.length()-1);
        }
        return str;
    }

    /**
     * 功能八 按照指定的分隔符，把字符串的所有key value 封装到map中
     * @param str 传入的字符串
     * @param regex
     * @return
     */
    public static Map<String,String> getMap(String str,String regex){
        String[] line = str.split("\\"+regex);
        HashMap<String, String> map = new HashMap<>();
        for (String fields : line) {
            String[] split = fields.split("=");
            if (split.length == 2) {
                String fieldName = split[0];
                String fieldValue = split[1];
                map.put(fieldName,fieldValue);
            }else {
                String fieldName = split[0];
                map.put(fieldName,"null");
            }
        }
        return map;
    }

    /**
     * 给数字补全位数（两位）
     * @param  str
     */

    public static String complementStr(String str){
        if (str.length()==1){
            return "0"+str;
        }
        return str;
    }
}
