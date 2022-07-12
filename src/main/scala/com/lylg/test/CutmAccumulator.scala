package com.lylg.test

import org.apache.spark.util.AccumulatorV2

import java.util

/** *
 * Author Mr. Guo
 * Create 2020/7/1 - 20:52
 */
/**
 * 自定义累加器
 */
object CustomAccumulator {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getSimpleName)
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    // 创建String 类型的累加器
    val rdd = sc.makeRDD(List("clickhouse", "kylin", "phoenix", "hive", "hbase", "spark", "hadoop"), 3)

    // 创建累加器
    val strAccu1 = new CutmAccumulator
    val strAccu2 = new CutmAccumulator
    // 注册累加器
    sc.register(strAccu1)
    sc.register(strAccu2)

    rdd.foreach(x => {
      strAccu1.add(x)
    })

    rdd.foreach(x => {
      strAccu2.add(x, "o")
    })

    println(strAccu1.value.toArray().toBuffer)
    println(strAccu2.value.toArray().toBuffer)

    sc.stop()
  }
}

class CutmAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  val list = new util.ArrayList[String]()

  // 当前的累加器是否为初始化状态,只需要判断一下list是否为空即可
  override def isZero: Boolean = list.isEmpty

  // 复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new CutmAccumulator
  }

  // 重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  // 向累加器中增加值
  override def add(v: String): Unit = {
    list.add(v)
  }

  // 重写方法，实现自定义业务
  def add(v: String, t: String): Unit = {
    if (v.contains(t)) {
      list.add(v)
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的结果
  override def value: util.ArrayList[String] = list
}