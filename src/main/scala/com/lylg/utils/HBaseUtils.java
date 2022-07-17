package com.lylg.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseUtils {


    static Configuration configuration = null;
    private static HBaseUtils instance = null;
    HBaseAdmin admin = null;

    /**
     * 私有改造方法
     */
    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "lylg102:2181,lylg103:2181,lylg104:2181");
        configuration.set("hbase.rootdir", "hdfs://lylg102:8020/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized HBaseUtils getInstance() {
        if (null == instance) {
            synchronized (HBaseUtils.class) {
                if (null == instance) {
                    instance = new HBaseUtils();
                }
            }

        }
        return instance;
    }
    public  Configuration getConf() {

        return configuration;
    }



    /**
     * 根据表名获取到HTable实例
     */
    public HTable getTable(String tableName) {

        HTable table = null;

        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    //只传表名得到全表数据
    public void scan(String tableName) throws IOException {
        System.out.println("******************\t"+tableName+"\t******************");
        // 2.获取表
        Table table = HBaseUtils.getInstance().getTable(tableName);

        // 3.全表扫描
        Scan scan = new Scan();

        // 4.获取扫描结果
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;

        // 5. 迭代数据
        while ((result = scanner.next()) != null) {
            // 6.打印数据 获取所有的单元格
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                // 打印rowkey,family,qualifier,value
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        + "==> " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "{" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + ":" + Bytes.toLong(CellUtil.cloneValue(cell)) + "}");
            }
        }
    }

    public Map<String, Long> query(String tableName, String rowkey) throws Exception {
        Map<String, Long> map = new HashMap<String, Long>();

        HTable table = getTable(tableName);
        String cf = "info";
        String qualifier = "count";

        Scan scan = new Scan();
        Filter filter = new PrefixFilter(Bytes.toBytes(rowkey));
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            String row = Bytes.toString(result.getRow());
            long count_ = Bytes.toLong(result.getValue(cf.getBytes(), qualifier.getBytes()));
            map.put(row, count_);
        }
        return map;

    }
    public static void main(String[] args) throws Exception {

        //HTable table = HBaseUtils.getInstance().getTable("imooc_course_clickcount");
        //System.out.println(table.getName().getNameAsString());
//        HBaseUtils.getInstance().put("lylg_visit", "2022-07-09", "info", "ts", "3");
//        Map<String, Long> map1 = HBaseUtils.getInstance().query("lylg_visit", "2022-07-09");
//        for (Map.Entry<String, Long> entry : map1.entrySet()) {
//            System.out.println(entry.getKey() + ": " + entry.getValue());
//        }
//
//        Map<String, Long> map2 = HBaseUtils.getInstance().query("lylg_active", "2022-07-11");
//        for (Map.Entry<String, Long> entry : map2.entrySet()) {
//            System.out.println(entry.getKey() + ": " + entry.getValue());
//        }

        System.out.println("查询全表数据");
        HBaseUtils.getInstance().scan("lylg_visit");
        HBaseUtils.getInstance().scan("lylg_register");
        HBaseUtils.getInstance().scan("lylg_active");
        HBaseUtils.getInstance().scan("lylg_select");
        HBaseUtils.getInstance().scan("lylg_total");


//        String tableName = "imooc_course_clickcount";
//        String rowkey = "20171111_88";
//        String cf = "info";
//        String column = "click_count";
//        String value = "2";
//
//        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);
    }

    /**
     * 添加一条记录到HBase表
     *
     * @param tableName HBase表名
     * @param rowkey    HBase表的rowkey
     * @param cf        HBase表的columnfamily
     * @param column    HBase表的列
     * @param value     写入HBase表的值
     */
    public void put(String tableName, String rowkey, String cf, String column, String value) {
        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
