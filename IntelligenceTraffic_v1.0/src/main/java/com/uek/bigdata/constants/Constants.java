package com.uek.bigdata.constants;

/**
 * 类名称：Constants
 * 类描述：项目常量接口
 *      功能：在整个项目开发过程中，为了避免硬编码，回吧变量值尽可能的进行抽取
 *            然后封装成一个常量接口，好处就是，在项目需要修改配置时，之间修改常量接口就行
 * @author 武建谦
 * @Time 2018/11/23 15:29
 */
public interface Constants {
    /**
     * 关系型数据库、JDBC相关配置
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";

}
