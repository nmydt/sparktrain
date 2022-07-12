package com.lylg.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, DriverManager, ResultSet, Statement}

/**
 * Description: Spark Streaming 完成有状态词频统计,并将结果写入MySQL
 */
object ForeachRDDApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")
    /**
     * Description: 创建StreamingContext ,conf: SparkConf, batchDuration: Duration
     */
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint(".")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6789)
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    val state = result.updateStateByKey(updateFunction _)
    state.print()

    state.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {

        val connection: Connection = createConnection()


        partition.foreach(record => {

          val sql = "select wordcount from wordcount where word='%s'".format(record._1)
          val sql1 = "insert into wordcount(word,wordcount) values('%s',%d)".format(record._1, record._2)
          val sql2 = "update wordcount set wordcount=%d where word='%s'".format(record._2, record._1)

          val statement: Statement = connection.createStatement()
          statement.execute(sql, ResultSet.TYPE_SCROLL_INSENSITIVE)
          val results: ResultSet = statement.getResultSet()
          // 将游标移动到最后一行上
          results.last();

          // 得到当前的 row number，在 JDBC 中，row number 从1开始，所以这里就相当于行数
          val rowCount = results.getRow();
          //此时游标执行了最后一行，如果我们后面还想从头开始调用 next()遍历整个结果集，我们可以将游标移动到第一行前面

          results.beforeFirst()

          if (rowCount == 0) { //数据库无此记录，插入

            statement.execute(sql1)
          } else { //数据库有此记录，更新

            statement.execute(sql2)
          }

        })
        connection.close()

      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
   * Description: 创建连接
   */
  def createConnection(): Connection = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/imooc_spark?useUnicode=true&amp&characterEncoding=utf8&useSSL=false&serverTimezone=GMT%2B8", "root", "123456")

  }

  /**
   * 把当前的数据去更新已有的或者是老的数据
   *
   * @param currentValues 当前的
   * @param preValues     老的
   * @return
   */

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

}
