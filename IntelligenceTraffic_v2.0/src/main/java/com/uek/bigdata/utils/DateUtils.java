package com.uek.bigdata.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 需求：
 * 时间工具类
 *
 * @author 武建谦
 * @Time 2018/11/23 14:01
 */
public class DateUtils {

    //定义三种时间格式 yyyy-MMdd HH:mm:ss   yyyy-MM-dd yyyyMMdd
    public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MMdd HH:mm:ss");
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");

    /**
     * 判断一个时间是否在另一个时间之前
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean before(String time1, String time2) throws ParseException {
        //将传递的字符串转换为日期
        Date date1 = TIME_FORMAT.parse(time1);
        Date date2 = TIME_FORMAT.parse(time2);
        //如果第一个时间在第二个时间之前则true 否则 false
        return date1.before(date2);
    }

    /**
     * 判断一个时间是否在另一个时间之后
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean after(String time1, String time2) throws ParseException {
        //将传递的字符串转换为日期
        Date date1 = TIME_FORMAT.parse(time1);
        Date date2 = TIME_FORMAT.parse(time2);
        //如果第一个时间在第二个时间之后则true 否则 false
        return date1.after(date2);
    }

    /**
     * 计算时间差值，单位为秒
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 差值
     */
    public static int minus(String time1, String time2) throws ParseException {
        //将传递的字符串转换为日期
        Date date1 = TIME_FORMAT.parse(time1);
        Date date2 = TIME_FORMAT.parse(time2);
        int minus = (int) Math.abs(date1.getTime() - date2.getTime()) / 1000;
        return minus;
    }

    /**
     * 获取字符串中的年月日和小时
     *
     * @param time 字符串中的时间
     * @return
     */
    public static String getDateHour(String time) {
        //切割
        String date = time.split(" ")[0];
        String temp = time.split(" ")[1];
        String hour = temp.split(":")[0];
        return date + "_" + hour;
    }


    /**
     * 获取今天的日期
     *
     * @return yyyy=-MM-dd
     */
    public static String getTodayDate() {
        return DATE_FORMAT.format(new Date());
    }


    /**
     * 获取昨天的日期
     *
     * @return yyyy=-MM-dd
     */
    public static String getYesterdayDate() {
        Calendar calendar = Calendar.getInstance();
        //设置时间
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        Date date = calendar.getTime();
        return DATE_FORMAT.format(date);
    }

    /**
     * 功能七：格式化日期
     *
     * @param date date对象
     * @return yyyy-MM-dd
     */
    public static String formatDate(Date date) {
        return DATE_FORMAT.format(date);
    }

    /**
     * 功能八：格式化时间
     *
     * @param date date对象
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String formatTime(Date date) {
        return TIME_FORMAT.format(date);
    }

    /**
     * 解析时间
     *
     * @param time 时间字符串
     * @return date对象
     */
    public static Date parseTime(String time) {
        try {
            return TIME_FORMAT.parse(time);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 格式化日期key
     *
     * @param date date对象
     * @return yyyyMMdd
     */
    public static String formatDateKey(Date date) {
        return DATEKEY_FORMAT.format(date);
    }

    public static Date parseDateKey(String time) {
        try {
            return DATEKEY_FORMAT.parse(time);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 格式化日期，保留到分钟级别
     *
     * @param date date对象
     * @return yyyyMMddHHmm
     */
    public static String fromatTimeMinute(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(date);
    }

    /**
     * 功能13 获取到一个时间范围
     *
     * @param time yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static String getRangTime(String time) {
        return null;
    }
}
