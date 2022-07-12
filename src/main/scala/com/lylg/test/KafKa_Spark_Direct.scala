package com.lylg.test
import com.google.gson.JsonObject
import com.lylg.Bean._
import com.lylg.dao._
import com.lylg.utils.{DateUtils, JsonUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object KafKa_Spark_Direct {
  def main(args: Array[String]): Unit = {
    //    System.setProperty("hadoop.home.dir", "C:\\winutils")
    if (args.length != 4) {
      System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
    }
    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // TODO... Spark Streaming如何对接Kafka
    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    val sc: SparkContext = ssc.sparkContext

    // 数据清洗
    val logs = messages.map(_._2)
    val cleanData: DStream[JsonObject] = logs.map(JsonUtils.gson(_)).filter(_.get("type_id") != null)

    //1, 日访问量  =>  当日访问次数   表名：lylg_visit
    // rowkey: timestamp  count：当日访问总次数  distinct_count: 当日用户上线数
    //count
    val value1: DStream[(String, Int)] = cleanData.filter(_.get("type_id").toString.toInt == 0).map(
      x => {
        (DateUtils.tranTimeToString(x.get("timestamp").toString), 1)
      }
    ).reduceByKey(_ + _).persist()
    // 当日访问总次数
    value1.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[VisitCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(VisitCount(pair._1, pair._2))
        })
        VisitCountDAO.save(list)
      })

    })
    //distinct count  每天有多少不同的用户访问或当日用户上线数
    val value11: DStream[(String, Int)] = cleanData.filter(_.get("type_id").toString.toInt == 0).map(
      x => {
        ((DateUtils.tranTimeToString(x.get("timestamp").toString), x.get("user_id").toString), 1)
      }
    ).reduceByKey((_, _) => 1).map(x => (x._1._1, x._2)).persist()

    value11.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[VisitCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(VisitCount(pair._1, pair._2))
        })
        VisitCount2DAO.save(list)
      })

    })
    // 从开始到现在为止总访问量
    value1.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[TotalCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(TotalCount("total_visit", pair._2))
        })
        TotalCountDAO.save(list)
      })

    })
    // 从开始到现在为止每天上线的用户总和
    value11.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[TotalCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(TotalCount("total_visit_distinct", pair._2))
        })
        TotalCountDAO.save(list)
      })

    })
    //2, 当日注册  => 当日注册量     表名： lylg_register
    // rowkey: timestamp  count：当日注册总次数
    val value2: DStream[(String, Int)] = cleanData.filter(_.get("type_id").toString.toInt == 1).map(
      x => {
        (DateUtils.tranTimeToString(x.get("timestamp").toString), 1)
      }
    ).reduceByKey(_ + _).persist()
    //当日注册总次数
    value2.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[RegisterCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(RegisterCount(pair._1, pair._2))
        })
        RegisterCountDAO.save(list)
      })

    })
    //从之前到现在为止注册总次数
    value2.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[TotalCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(TotalCount("total_register", pair._2))
        })
        TotalCountDAO.save(list)
      })

    })
    //3, 活跃度   =>文章显示次数    表名： lylg_active
    // rowkey: timestamp + article_id  count：当日各个文章打开总次数
    val value3: DStream[(String, Int)] = cleanData.filter(_.get("type_id").toString.toInt == 2).map(
      x => {
        (DateUtils.tranTimeToString(x.get("timestamp").toString) + "_" + x.get("article_id"), 1)
      }
    ).reduceByKey(_ + _).persist()
    //当日各个文章打开总次数
    value3.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[ActiveCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(ActiveCount(pair._1, pair._2))
        })
        ActiveCountDAO.save(list)
      })

    })
    //从之前到现在为止文章打开总次数
    value3.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[TotalCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(TotalCount("total_active", pair._2))
        })
        TotalCountDAO.save(list)
      })

    })
    //各个文章从之前到现在访问总次数
    val value31: DStream[(String, Int)] = cleanData.filter(_.get("type_id").toString.toInt == 2).map(
      x => {
        (x.get("article_id").toString, 1)
      }
    ).reduceByKey(_ + _).persist()
    value31.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[TotalCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(TotalCount(pair._1, pair._2))
        })
        TotalCountDAO.save(list)
      })

    })
    //4, 搜索     => 搜索量       表名： lylg_select
    // rowkey: timestamp + md5(search_content)  count：总搜索量
    val value4: DStream[(String, Int)] = cleanData.filter(_.get("type_id").toString.toInt == 3).map(
      x => {
        (x.get("search_content").toString, 1)
      }
    ).reduceByKey(_ + _).persist()
    //当日搜索量
    value4.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[SelectCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(SelectCount(pair._1, pair._2))
        })
        SelectCountDAO.save(list)
      })

    })
    //从之前到现在总的搜索量
    value4.foreachRDD((rdd: RDD[(String, Int)]) => {
      val list = new ListBuffer[TotalCount]
      rdd.foreachPartition(partition => {

        partition.foreach(pair => {
          println(pair._1)
          println(pair._2)
          list.append(TotalCount("total_search", pair._2))
        })
        TotalCountDAO.save(list)
      })

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
