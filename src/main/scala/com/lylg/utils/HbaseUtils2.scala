package com.lylg.utils

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import java.io.IOException
import scala.collection.mutable.ListBuffer

/**
 * 从hbase中增删改查数据
 *
 */
object HbaseUtils2 {
  val configuration = HBaseConfiguration.create()
  //  configuration.set ("zookeeper.znode.parent", "/hbase-unsecure") //看情况有时候要加有时候不加
  //  configuration.set("hbase.master","lylg102:60010")
  val connection = ConnectionFactory.createConnection(configuration)
  configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
  val admin = connection.getAdmin
  var zookeeperQuorum = "lylg102:2181,lylg103:2181,lylg104:2181"

  def isExists(tableName: String): Boolean = {
    var result = false
    val tName = TableName.valueOf(tableName)
    if (admin.tableExists(tName)) {
      result = true
    }
    result
  }

  //创建一个hbase表
  def createTable(tableName: String, columnFamilys: Array[String]) = {
    //操作的表名
    val tName = TableName.valueOf(tableName)
    //当表不存在的时候创建Hbase表
    if (!admin.tableExists(tName)) {
      //创建Hbase表模式
      val descriptor = new HTableDescriptor(tName)
      //创建列簇i
      for (columnFamily <- columnFamilys) {
        descriptor.addFamily(new HColumnDescriptor(columnFamily))
      }
      //创建表
      admin.createTable(descriptor)
      println("create successful!!")
    }
  }

  def dropTable(tableName: String): Unit = {
    admin.disableTable(TableName.valueOf(tableName))
    admin.deleteTable(TableName.valueOf(tableName))
    println("drop successful!!")
  }

  //向hbase表中插入数据
  //put 'sk:test1','1','i:name','Luck2'
  def insertTable(tableName: String, rowkey: String, columnFamily: String, column: String, value: String) = {

    val table = connection.getTable(TableName.valueOf(tableName))
    //准备key 的数据
    val puts = new Put(rowkey.getBytes())
    //添加列簇名,字段名,字段值value
    puts addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
    //把数据插入到tbale中
    table.put(puts)
    println("insert successful!!")
  }

  // insertTable("stu" ,(001,1,2,3),"rowkey,info:value,info:region,info:namespace")

  //  insertTable("stu" ,"rowkey,info:value,info:region,info:namespace",("001,1,2,3","..."))
  def insertTable(tableName: String, columnFamilyAndcolumn: String, array: Array[String]): Unit.type = {
    array.map(row => {
      val values = row.split(",")
      insertTable(tableName, values, columnFamilyAndcolumn)
    })
    return Unit
  }

  def insertTable(tableName: String, values: Array[String], columnFamilyAndcolumn: String) = {
    // (rowkey,(info,age),(info,id))
    var index = 0
    val rowkey = values(index)
    val columns: Array[Array[String]] = columnFamilyAndcolumn.split(",").map(_.split(":")).slice(1, values.size)
    val table = connection.getTable(TableName.valueOf(tableName))
    columns.map(column_ => {
      //准备key 的数据
      val puts = new Put(rowkey.getBytes());

      //添加列簇名,字段名,字段值value
      val columnFamily = column_(0);
      val column = column_(1);
      val value = values(index)
      index += 1
      puts.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
      //把数据插入到tbale中
      table.put(puts)

    })
    println("insert successful!!")
  }

  //删除某条记录
  //delete 'sk:test1','1','i:name'
  def deleteRecord(tableName: String, rowkey: String, columnFamily: String, column: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))

    val info = new Delete(Bytes.toBytes(rowkey))
    info.addColumn(columnFamily.getBytes(), column.getBytes())
    table.delete(info)
    println("delete successful!!")
  }

  def main(args: Array[String]): Unit = {
    var arr = new Array[String](1)
    arr(0) = "info1"
    //    createTable("user2",arr)
    scanDataFromHTable("lylg_visit", "info", "count")
    getAllData("lylg_visit").foreach(println)
    //insertTable("user2","1","info1","name","lyh")
    //scanDataFromHTable("user2","info1","name")
    //deleteRecord("user2","1","info1","name")
    //    dropTable("user2")
    //    println(isExists("user2"))
    close()
  }

  //获取hbase表中的数据
  //scan 'sk:test1'
  def scanDataFromHTable(tableName: String, columnFamily: String, column: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    //定义scan对象
    val scan = new Scan()
    //添加列簇名称
    scan.addFamily(columnFamily.getBytes())
    //从table中抓取数据来scan
    val scanner = table.getScanner(scan)
    var result = scanner.next()
    //数据不为空时输出数据
    while (result != null) {
      println(s"rowkey:${Bytes.toString(result.getRow)},列簇:${columnFamily}:${column},value:${Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))}")
      result = scanner.next()
    }
    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()
  }

  //只传表名得到全表数据
  def getAllData(tableName: String): ListBuffer[String] = {
    var table: Table = null
    val list = new ListBuffer[String]
    try {
      table = connection.getTable(TableName.valueOf(tableName))
      val results: ResultScanner = table.getScanner(new Scan)
      import scala.collection.JavaConversions._
      for (result <- results) {
        for (cell <- result.rawCells) {
          val row: String = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
          val family: String = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
          val colName: String = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val value: String = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          val context: String = "rowkey:" + row + "," + "列族:" + family + "," + "列:" + colName + "," + "值:" + value
          list += context
        }
      }
      results.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    list
  }

  // 关闭 connection 连接
  def close() = {
    if (connection != null) {
      try {
        connection.close()
        println("关闭成功!")
      } catch {
        case e: IOException => println("关闭失败!")
      }
    }
  }
}
