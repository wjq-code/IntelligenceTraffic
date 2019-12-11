package com.uek.bigdata.utils

/**
  * 类名称:StringUtil
  * 类描述:
  *
  * @Time 2018/11/25 23:52
  * @author 武建谦
  */

object StringUtils{

  /**
    * @param str 传入一个字符串类型的参数
    * @return 判断是否为空
    */
  def isEmpty(str: String): Boolean = "".eq(str) || str == null

  /**
    * @param str 传入一个字符串类型的参数
    * @return 判断是否不为空
    */
  def isNotEmpty(str: String): Boolean = !"".eq(str) || str != null

  /**
    * 对传入的形参进行补全n为数字
    *
    * @param num 指定补全的位数
    * @param str 指定补全的字符传
    * @return 补全后的结果
    */
  def complementStr(num: Int, str: String): String = {
    if (num == str.length) {
      str
    } else {
      var sb = new StringBuilder
      val n = num - str.length
      for (i <- 1 to n) {
        sb ++= "0"
      }
      sb ++= str
      sb.toString()
    }
  }

  /**
    * name=zhangshengxiao|age=25|sex=man|hunfu=
    *
    * @param str   传入的字符串
    * @param regex 传入的分隔符
    * @param field 指定字段名
    * @return 返回字段名所对的value值
    */
  def extractValue(str: String, regex: String, field: String): String = {
    val line = str.split("\\" + regex)
    for (fields <- line) {
      val split = fields.split("=")
      if (split.length == 2) {
        val fieldName = split(0)
        val fieldValue = split(1)
        if (field == fieldName) return fieldValue
      }
    }
    null
  }
  /**
    * 给指定的字段设置值，
    * name=zhangshengxiao|age=25|sex=man|hunfu=
    *
    * @param str       传入的字符串
    * @param regex     指定分隔符
    * @param fieldName 指定的字段名
    * @param fieldVale 字段名对应的值
    * @return 返回一个更新后的字符串
    */
  def setFieldValue(str: String, regex: String, fieldName: String, fieldVale: String): String = {
    val split = str.split("\\" + regex)
    val sb = new StringBuilder
    for (fields <- split) {
      val field = fields.split("=")
      val name = field(0)
      if (name == fieldName) sb.append(fieldName + "=" + fieldVale + "|")
      else {
        sb.append(fields).append("|")
      }
    }
    sb.toString().substring(0, sb.toString().length - 1)
  }

  /**
    * @param str 传入的字符串
    * @return 字符串对应的integer类型
    */
  def str2Int(str: String): Int = str.toInt

  /**
    *
    * @param str 出入字符串
    * @return 截断字符串两侧的，
    */
  def trimStr(str: String): String = {
    var str1 = str
    if (str1.startsWith(",")) str1 = str.substring(1)
    if (str1.endsWith(",")) str1.substring(0, str1.length - 1)
    str1
  }

  /**
    * 功能八 按照指定的分隔符，把字符串的所有key value 封装到map中
    *
    * @param str 传入的字符串
    * @param regex
    * @return
    */
  def getMap(str: String, regex: String): Map[String, String] = {
    val split = str.split("\\" + regex)
    var map: Map[String, String] = Map()
    for (fields <- split) {
      val field = fields.split("=")
      if (field.length == 2) {
        val fieldName = field(0)
        val fieldValue = field(1)
        map += (fieldName -> fieldValue)
      }
    }
    map
  }

  /**
    * 给数字补全位数（两位）
    *
    * @param  str
    */
  def complementStr(str: String): String = {
    if (str.length == 1) "0" + str
    else str
  }
}
