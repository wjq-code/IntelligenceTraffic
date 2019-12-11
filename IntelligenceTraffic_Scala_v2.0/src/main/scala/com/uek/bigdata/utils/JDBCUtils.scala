package com.uek.bigdata.utils

import java.sql.{Connection, DriverManager, ResultSet}
import java.util

import com.uek.bigdata.config.ConfigurationManager
import com.uek.bigdata.constans.Constants
import com.uek.bigdata.utils.JDBCUtils.QueryCallBack

import scala.collection.mutable.ListBuffer

/**
  * 类名称: JDBCUtils
  * 类描述:
  *
  * @Time 2018/11/26 22:46
  * @author 武建谦
  */
object JDBCUtils {
  private var instance:JDBCUtils = null
  def getInstance():JDBCUtils={
    if (instance == null){
      JDBCUtils.synchronized{
        if (instance == null){
          instance = new JDBCUtils
        }
      }
    }
    instance
  }
  //回调函数
  trait QueryCallBack{
    def process(rs:ResultSet)
  }

}
class JDBCUtils{
  //通过读取配置文件来获取JDBC相关配置
  private val driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER)
  Class.forName(driver)
  /**
    * 构建数据库连接池
    */
  private val dateSource = new util.LinkedList[Connection]()
  //获取线程池的数量
  private var count = ConfigurationManager.getInt(Constants.JDBC_DATASOURCE_SIZE)

  for (i <- 1 until count) {
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    val user = ConfigurationManager.getProperty(Constants.JDBC_USER)
    val password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)

    val conn = DriverManager.getConnection(url, user, password)
    dateSource.push(conn)
  }

  /**
    * 获取数据库连接
    * @return 数据量连接对象
    */
  private def getConnection(): Connection = {
    AnyRef.synchronized({
      if (dateSource.size == 0) {
        Thread.sleep(1000)
      }
    })
    dateSource.poll()
  }

  /**
    * 业务操作CRUD
    */
  def executorUpdate(sql:String,param:Array[Any]):Int={
    var conn = getConnection()
    //开启事物，取消自动提交
    conn.setAutoCommit(false)
    val pstmt = conn.prepareStatement(sql)
    for (i <- 0 until param.length){
      pstmt.setObject(i+1 , param(i))
    }
    val status = pstmt.executeUpdate()
    conn.commit()
    pstmt.close()
    dateSource.push(conn)
    status
  }

  /**
    * 查询功能
    * @param sql
    * @param param
    * @param queryCallBack
    */
  def executorQurey(sql:String,param:Array[Any],queryCallBack: QueryCallBack)={
    val conn = getConnection()
    val pstmt = conn.prepareStatement(sql)
    for (i <- 0 until param.length){
      pstmt.setObject(i + 1,param(i))
    }
    val rs = pstmt.executeQuery()
    queryCallBack.process(rs)
    rs.close()
    pstmt.close()
    dateSource.push(conn)
  }



  /**
    * 批量插入
    */
  def executeBatch(sql:String,params:ListBuffer[Array[Any]]):Array[Int] = {
    val conn = getConnection()
    //开启手动提交事物
    conn.setAutoCommit(false)
    val pstmt = conn.prepareStatement(sql)
    if (params.size > 0 && params != null){
      for (param <- params) {
        for (i <- 0 until param.length) {
          pstmt.setObject(i+1,param(i))
        }
        pstmt.addBatch()
      }
    }
    val ints = pstmt.executeBatch()
    conn.commit()
    ints
  }
}
