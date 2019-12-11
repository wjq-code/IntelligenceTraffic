package com.uek.bigdata.test

import com.uek.bigdata.DAO.factory.DAOFactory
import com.uek.bigdata.config.ConfigurationManager
import com.uek.bigdata.constans.Constants
import com.uek.bigdata.utils.{JDBCUtils, StringUtils}

import scala.collection.mutable.ListBuffer

/**
  * 类名称: Test
  * 类描述:
  *
  * @Time 2018/11/28 23:53
  * @author 武建谦
  */
object Test {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5)
    val list1 = new ListBuffer[Int]
    list1 += (1,2,3,4,5)
    val listStr = list1.mkString("[",",","]")
    println(listStr)
    println(list.mkString(","))
  }
}
