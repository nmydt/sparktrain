package com.lylg.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test2 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val sc = new SparkContext(conf)
    var array: Array[String] = Array("003,beijing,23,lili", "004,beijing,23,lili", "005,beijing,23,lili")

    val rdd: RDD[String] = sc.parallelize(array)
    array :+= "加入这个元素"
    rdd.foreach(x => {
      println("***")
      array :+= "我想加入这个元素"
    })
    array.foreach(println)

    val l = List(1, 2, 3)
    var ll = l.map(x => x * x)


  }
}