package com.uek.bigdata.utils

import com.alibaba.fastjson.JSONObject
import com.google.gson.{JsonObject, JsonParser}
import com.uek.bigdata.config.ConfigurationManager
import com.uek.bigdata.constans.Constants

/**
  * 类名称: ParamsUtils
  * 类描述: 用于解析json数据，获取指定的内容
  *
  * @Time 2018/12/1 9:31
  * @author 武建谦
  */
object ParamsUtils {
  /**
    * 从命令参数中提取taskId
    *
    * @param args
    * @param takeType
    * @return
    */
  def getTaskId(args: Array[String], takeType: String): Long = {
    //判断是或否为本地运行模式
    val flag = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    val taskId: Long = if (flag) {
      //本地运行模式
      ConfigurationManager.getLong(takeType)
    } else {
      if (args != null && args.length > 0) {
        args(0).toLong
      }
      0L
    }
    taskId
  }

  /**
    * 从json数据中获取到指定的信息
    *
    * @param jsonObject
    * @param fieldName
    */
  def getParamsByJsonStr(jsonStr: String, fieldName: String): String = {
    val json = new JsonParser
    val obj = json.parse(jsonStr).asInstanceOf[JsonObject]
    val value = obj.get(fieldName)
    value.toString.substring(2,value.toString.length-2)
  }
}

