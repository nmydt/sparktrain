package com.lylg.utils

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.time.LocalTime
import java.util.{Calendar, Date}
import scala.io.Source

object DateUtils {
  def main(args: Array[String]): Unit = {
    val tm = "1657349003351"

    val a = tranTimeToString(tm)

    //    getYesteday()
    //    println(getTimeStamp())
    //从文件中读取数据
    var builder = new StringBuilder()
    Source.fromFile("C:\\Users\\DXG\\IdeaProjects\\sparktrain-master\\global.txt").foreach((x: Char) =>{
      var y = x.toString
      builder.append(y)
    })
    val timestamp = builder.toString()
    println(timestamp)
    print("上次执行时间为：" + DateUtils.tranTimeToString(timestamp))
    if (timestamp.toLong < DateUtils.getTimeStamp()) {
      //将数据写入文件
      print("将下一次执行时间写入文件")
      val writer = new PrintWriter(new File("global.txt"))
      writer.write(DateUtils.getTimeStamp().toString)
      writer.close()
    }


  }


  def tranTimeToString(tm: String): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }

  def getYesteday(): String = {

    val date = getToday()
    val myformat = new SimpleDateFormat("yyyy-MM-dd")
    var dnow = new Date()
    dnow = myformat.parse(date)
    val cal1 = Calendar.getInstance()
    cal1.setTime(dnow)
    cal1.add(Calendar.DATE, -1)
    myformat.format(cal1.getTime())
  }

  def getToday(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cla = Calendar.getInstance()
    cla.setTimeInMillis(System.currentTimeMillis())
    val date = dateFormat.format(cla.getTime)
    date
  }

  def getTimeStamp() = {
    val now = new Date()
    now.getTime
  }

  def getZero(): Long = {
    val second: Long = 86400000000000L - LocalTime.now.toNanoOfDay
    (second.toDouble / 1000 / 1000).toLong
  }
}
