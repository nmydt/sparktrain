package com.lylg.dao

import com.lylg.Bean.SelectCount
import com.lylg.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * Description: 数据访问层：用于注册量的数据保存
 */
object SelectCountDAO {
  val tableName = "lylg_select"
  val cf = "info"
  val qualifer = "count"

  /**
   * 根据rowkey查询值
   */
  def count(day_course: String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {


    val list = new ListBuffer[SelectCount]
    list.append(SelectCount("2022-07-08", 8))
    //    list.append(VisitCount("20171111_9",9))
    //    list.append(VisitCount("20171111_1",100))

    save(list)

    //    println(count("20171111_8") + " : " + count("20171111_9")+ " : " + count("20171111_1"))
  }

  /**
   * 保存数据到HBase
   *
   * @param list VisitCount集合
   */
  def save(list: ListBuffer[SelectCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.date),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.count)
    }

  }

}
