package com.lylg.test

import ForeachRDDApp.createConnection

import java.sql.{Connection, Statement}

/**
 * Description: execute与executeUpdate的区别: https://blog.csdn.net/wo_shi_LTB/article/details/79022773
 * Java JDBC 中获取 ResultSet 的大小: https://blog.csdn.net/DrifterJ/article/details/17720271
 */


object practice {
  def main(args: Array[String]): Unit = {
    // 插入
    val sql1 = "insert into wordcount(word,wordcount) values('%s',%d)".format("a", 1)
    println(sql1)
    //更新
    val sql2 = "update wordcount set wordcount=%d where word='%s'".format(11, "a")
    print(sql2)
    //插入主键
    //    先删除主键，然后再增加主键，注:在增加主键之前,必须先把反复的id删除掉。
    //    alter table table_test drop primary key;

    val sql3 = "alter table wordcount add primary key(word)"
    val connection: Connection = createConnection()

    val sql = "insert into wordcount(word,wordcount) values('%s',%d)".format("a", 3)
    val statement: Statement = connection.createStatement()
    statement.execute("select wordcount from wordcount where word='a'")
    statement.getResultSet()

    connection.close()
  }

}
