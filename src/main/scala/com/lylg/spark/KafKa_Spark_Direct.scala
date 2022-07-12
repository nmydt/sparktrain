package com.lylg.spark

import com.google.gson.JsonObject
import com.lylg.Bean._
import com.lylg.dao._
import com.lylg.utils.{DateUtils, HBaseUtils, JsonUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.io.Source

object KafKa_Spark_Direct {
  def main(args: Array[String]): Unit = {
//        System.setProperty("hadoop.home.dir", "C:\\winutils")
    if (args.length != 4) {
      System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
    }
    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount")//.setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    var global_time = -1L
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
    //判断是否执行ReadHBase

    cleanData.foreachRDD(x => {
      var timestamp = ""
      if(global_time == -1L){
        var builder = new StringBuilder()
        Source.fromFile("/home/lylg/top/global.txt").foreach((x: Char) => {
          var y = x.toString
          builder.append(y)
        })
        timestamp = builder.toString()
        global_time = timestamp.toLong
      }else{
        timestamp = global_time.toString
        print("已经从全局变量获得下次执行时间")
      }
      //从文件中读取数据

      println("执行时间："+timestamp)
      if (timestamp.toLong < DateUtils.getTimeStamp()) {
        println("下次执行时间已经过期，重置下次执行时间")
        global_time = DateUtils.getTimeStamp()+3600000L
        println("上次执行时间为：" + DateUtils.tranTimeToString(timestamp))
        //将数据写入文件
        println("将下一次执行时间写入文件")
        val writer = new PrintWriter(new File("/home/lylg/top/global.txt"))
        writer.write((DateUtils.getTimeStamp()+3600000L).toString)
        writer.close()

        val confHbase: Configuration = HBaseUtils.getInstance().getConf()
        getTopArticleCount(sc, confHbase, "lylg_total", "file:///home/lylg/top/lylg_total", 10)
        getTopSearchCount(sc, confHbase, "lylg_select", "file:///home/lylg/top/lylg_select", 10)

      }

    })

    ssc.start()
    ssc.awaitTermination()

  }

  def getTopSearchCount(sc: SparkContext, conf: Configuration, TableName: String, SavePath: String, TopNum: Int): Unit = {

    conf.set(TableInputFormat.INPUT_TABLE, TableName)
    // hbase
    val stuRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = stuRDD.count()
    println("Students RDD Count:" + count)

    //排序处理
    val result1: RDD[(Long, String)] = stuRDD.map(_._2).map(x => {
      val rowkey = Bytes.toString(x.getRow)
      val count = Bytes.toLong(x.getValue("info".getBytes, "count".getBytes))
      (count, rowkey)
    })

    val result2: RDD[(Long, String)] = result1.repartition(1)
    val result3: RDD[(Long, String)] = result2.sortByKey(false)
    val result4: RDD[String] = result3.map(x => x._2 + "," + x._1)
    //判断目录是否存在，存在就删除
    dirDel(new File("/home/lylg/top/lylg_select"))
    val result = sc.makeRDD(result4.take(TopNum)).repartition(1)
    result.saveAsTextFile(SavePath)


  }

  def getTopArticleCount(sc: SparkContext, conf: Configuration, TableName: String, SavePath: String, TopNum: Int): Unit = {

    conf.set(TableInputFormat.INPUT_TABLE, TableName)

    // hbase
    val stuRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = stuRDD.count()
    println("Students RDD Count:" + count)

    //过滤total_active
    val result1: RDD[(Long, String)] = stuRDD.map(_._2).map(x => {
      val rowkey = Bytes.toString(x.getRow)
      val count = Bytes.toLong(x.getValue("info".getBytes, "count".getBytes))
      (count, rowkey)
    }).filter(x => {
      val value: String = x._2
      !value.contains("total")
    })
    //排序处理
    val result2: RDD[(Long, String)] = result1.repartition(1)
    val result3: RDD[(Long, String)] = result2.sortByKey(false)
    val result4: RDD[String] = result3.map(x => x._2 + "," + x._1)
    //判断目录是否存在，存在就删除
    dirDel(new File("/home/lylg/top/lylg_total"))
    val result = sc.makeRDD(result4.take(TopNum)).repartition(1)
    result.saveAsTextFile(SavePath)


  }
  //删除目录和文件
  def dirDel(path: File) {
    if (!path.exists()) {
      println("目录不存在")
      return
    } else if (path.isFile()) {
      path.delete()
      println(path + ":  文件被删除")
      return
    }

    val file: Array[File] = path.listFiles()
    for (d <- file) {
      dirDel(d)
    }

    path.delete()
    println(path + ":  目录被删除")
  }

}
