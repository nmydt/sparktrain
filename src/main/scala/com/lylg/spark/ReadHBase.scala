package com.lylg.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object ReadHBase {
  val getInstance ={
//    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val sparkconf = new SparkConf().setAppName("HBaseRead")//.setMaster("local[2]")
    val sc = new SparkContext(sparkconf)
    //zkQuorum
    val zkHost = "lylg102:2181,lylg103:2181,lylg104:2181"
    val conf = HBaseConfiguration.create()
    //设置查询的表名
    conf.set("hbase.zookeeper.quorum", zkHost)
    //    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    (conf,sc)
  }
  def getTopSearchCount(TableName :String, SavePath :String, TopNum :Int): Unit ={
    val (conf,sc) = getInstance
    conf.set(TableInputFormat.INPUT_TABLE, TableName)
    // hbase
    val stuRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = stuRDD.count()
    println("Students RDD Count:" + count)
    stuRDD.cache()

    //排序处理
    val result1: RDD[(Long, String)] = stuRDD.map(_._2).map(x =>{
      val rowkey = Bytes.toString(x.getRow)
      val count = Bytes.toLong(x.getValue("info".getBytes, "count".getBytes))
      (count,rowkey)
    })

    val result2: RDD[(Long, String)] = result1.repartition(1)
    val result3: RDD[(Long, String)] = result2.sortByKey(false)
    val result4: RDD[String] = result3.map(x => x._2+","+x._1)
    //判断目录是否存在，存在就删除
    dirDel(new File("/home/lylg/top/lylg_select"))
    val result = sc.makeRDD(result4.take(TopNum)).repartition(1)
    result.saveAsTextFile(SavePath)


  }
  def getTopArticleCount(TableName :String, SavePath :String, TopNum :Int): Unit ={
    val (conf,sc) = getInstance

    conf.set(TableInputFormat.INPUT_TABLE, TableName)

    // hbase
    val stuRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = stuRDD.count()
    println("Students RDD Count:" + count)
    stuRDD.cache()

    //过滤total_active
    val result1: RDD[(Long, String)] = stuRDD.map(_._2).map(x =>{
      val rowkey = Bytes.toString(x.getRow)
      val count = Bytes.toLong(x.getValue("info".getBytes, "count".getBytes))
      (count,rowkey)
    }).filter(x =>{
      val value: String = x._2
      !value.contains("total")
    })
    //排序处理
    val result2: RDD[(Long, String)] = result1.repartition(1)
    val result3: RDD[(Long, String)] = result2.sortByKey(false)
    val result4: RDD[String] = result3.map(x => x._2+","+x._1)
    //判断目录是否存在，存在就删除
    dirDel(new File("/home/lylg/top/lylg_total"))
    val result = sc.makeRDD(result4.take(TopNum)).repartition(1)
    result.saveAsTextFile(SavePath)


  }
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: ReadHBase <TopNum>")
    }

    val Array(topnum) = args
    getTopArticleCount("lylg_total","file:///home/lylg/top/lylg_total",topnum.toInt)
    getTopSearchCount("lylg_select","file:///home/lylg/top/lylg_select",topnum.toInt)

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
