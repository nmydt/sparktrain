package com.lylg.utils

import com.google.gson.{JsonObject, JsonParser}

object JsonUtils {
  def main(args: Array[String]): Unit = {
    var array: Array[String] = Array("003,beijing,23,lili", "004,beijing,23,lili", "005,beijing,23,lili")
    //    array :+= "lalala"
    //
    //    array.foreach(println)
    //    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    //    val sc = new SparkContext(conf)
    //    val rdd: RDD[String] = sc.parallelize(array)
    //    //    rdd.map(_.toString())
    //    rdd.foreach(x => {
    //      println("00000")
    //      array :+ "1"
    //    })
    //    array.foreach(println)
    val jsonObject: JsonObject = gson("{'key':'snj','addres':'beijing','age':23,'username':'lili'}")
    println(jsonObject.get("key").getAsInt)
    print(1.getClass.getSimpleName)


  }

  def gson(str2: String): JsonObject = {
    val json = new JsonParser()
    val obj = json.parse(str2).asInstanceOf[JsonObject]
    return obj
  }

  def gson(str1: String, str2: String): StringBuilder = {
    val json = new JsonParser()
    val obj = json.parse(str2).asInstanceOf[JsonObject]
    val splits: Array[String] = str1.split(",")
    val len: Int = splits.length
    val str = new StringBuilder("")
    for (i <- 1 to len) {
      if (i == len) {
        str.append(obj.get(splits(len - 1)))
      } else {
        str.append(obj.get(splits(i - 1)) + ",")
      }
    }
    //    println(str.toString())
    return str
  }
}
