package com.lylg.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Description: Spark Streaming 处理socket数据
 */
object NetworkWordcount {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Winutils\\");

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordcount")
    /**
     * Description: 创建StreamingContext ,conf: SparkConf, batchDuration: Duration
     */
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6789)

    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
