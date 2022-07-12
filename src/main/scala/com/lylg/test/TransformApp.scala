package com.lylg.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Description: 黑名单过滤
 */
object TransformApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    /**
     * Description: 构建黑名单
     */
    val blacks = List("zs", "ls")
    val blackRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blacks).map((_, true))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6789)
    val clicklog: DStream[String] = lines.map(x => (x.split(" ")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blackRDD)
        .filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)
    })
    clicklog.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
