package com.uek.bigdata.config

import java.io.FileInputStream

import com.uek.bigdata.constans
import java.util.Properties

import com.uek.bigdata.constans.Constants

/**
  * 类名称: ConfigurationManager
  * 类描述:
  * 在开发项目的过程中，一般都会设计几个配置管理组件，
  * 这些配置管理组件，对整个项目的读取的配置信息，进行管理
  *
  * @Time 2018/11/28 20:03
  * @author 武建谦
  */
object ConfigurationManager {
  private val props = new Properties()
  //  val path = Thread.currentThread().getContextClassLoader.getResource("project.properties").getPath
  //  props.load(new FileInputStream(path))
  props.load(ConfigurationManager.getClass.getClassLoader.getResourceAsStream("project.properties"));

  /**
    * 功能1:通过key来回去对应的value值
    */
  def getProperty(key: String): String = {
    props.getProperty(key)
  }

  /**
    * 功能2：通过key获取对应的value，然后把value值转换integer
    */
  def getInt(key: String): Int = {
    try {
      props.getProperty(key).toInt
    } catch {
      case e: NumberFormatException => {
        0
      }
    }
  }

  /**
    * 功能3：通过key获取对应的value，然后把value值转换Boolean
    */
  def getBoolean(key: String): Boolean = {
    try {
      props.getProperty(key).toBoolean
    } catch {
      case e: NumberFormatException => {
        false
      }
    }
  }

  /**
    * 功能4：通过key获取对应的value，然后把value值转换Long
    */
  def getLong(key: String): Long = {
    try {
      props.getProperty(key).toLong
    } catch {
      case e: NumberFormatException => {
        0L
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(ConfigurationManager.getInt("jdbc.datasource.size"))
  }
}
