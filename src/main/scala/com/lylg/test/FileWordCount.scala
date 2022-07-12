package com.lylg.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Description: Spark Streaming 뇹잿HDFS뵨굶뒈鑒앴
 */
object FileWordCount {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Winutils\\");

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("FileWordCount")
    /**
     * Description: 눼쉔StreamingContext ,conf: SparkConf, batchDuration: Duration
     */
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines: DStream[String] = ssc.textFileStream("file:///F://test")

    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
