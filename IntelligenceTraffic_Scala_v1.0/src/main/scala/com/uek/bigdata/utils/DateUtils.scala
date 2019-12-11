package com.uek.bigdata.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * 类名称: DateUtils
  * 类描述:
  *
  * @Time 2018/11/26 10:44
  * @author 武建谦
  */
object DateUtils{

  //定义三种日期格式
  private val TIME_FORMAT:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val DATA_FORMAT:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val DATAKEY_FORMAT:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

  /**
    * 判断一个时间在不在另一个时间之前
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def before(time1:String,time2:String):Boolean={
    val date1 = TIME_FORMAT.parse(time1)
    val date2 = TIME_FORMAT.parse(time2)
    date1.before(date2)
  }

  /**
    * 判断一个时间在另一个时间之后
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def after(time1:String,time2:String):Boolean={
    val date1 = TIME_FORMAT.parse(time1)
    val date2 = TIME_FORMAT.parse(time2)
    date1.after(date2)
  }

  /**
    * 判断两个时间之差
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 两个时间段之间的差
    */
  def minus(time1:String,time2:String) :Int ={
    val date1 = TIME_FORMAT.parse(time1)
    val date2 = TIME_FORMAT.parse(time2)
    val minus = math.abs(date1.getTime - date2.getTime)
    (minus / 1000).toInt
  }

  /**
    * 获取日期中的年月日时
    * @param time 传入的时间
    * @return
    */
  def getDateHour(time:String) :String ={
    val date = time.split(" ")(0)
    val temp = time.split(" ")(1)
    val hour = temp.split(":")(0)
    date + "_" + hour
  }

  /**
    * 获取今天的日期
    * @return 当天日期
    */
  def getTodayDate():String ={
    DATA_FORMAT.format(new Date)
  }

  /**
    * 获取昨天的日期
    * @return
    */
  def getYesterdayDate():String={
    val calendar = Calendar.getInstance()
    calendar.setTime(new Date())
    calendar.add(Calendar.DAY_OF_YEAR,-1)
    DATA_FORMAT.format(calendar.getTime)
  }

  /**
    *
    * @param date 需要格式化的日期
    * @return yyyy-MM-dd
    */
  def formatDate(date:Date):String={
    DATA_FORMAT.format(date)
  }

  /**
    * 格式化日期
    * @param date
    * @return yyyy-MM-dd HH:mm:ss
    */
  def formatTime(date:Date):String={
    TIME_FORMAT.format(date)
  }

  /**
    * 格式化日期key
    * @param date date对象
    * @return yyyyMMdd
    */
  def formatDateKey(date: Date):String={
    DATAKEY_FORMAT.format(date)
  }


  /**
    * 解析时间
    * @param time 时间字符串
    * @return date对象
    */
  def parseTime(time:String):Date={
    TIME_FORMAT.parse(time)
  }

  def fromatTimeMinute(date: Date):String={
    val df =  new SimpleDateFormat("yyyyMMddmm")
    df.format(date)
  }
}