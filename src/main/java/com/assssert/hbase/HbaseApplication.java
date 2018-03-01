package com.assssert.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@SpringBootApplication
public class HbaseApplication {

    private Connection connection;
    private Admin admin;

    public static void main(String[] args) {
        SpringApplication.run(HbaseApplication.class, args);
    }

    @RequestMapping("/add")
    public void test() {
        init();
        // 数据表表名
        String tableNameString = "bbb";
        // 新建一个数据表表名对象
        TableName tableName = TableName.valueOf(tableNameString);
        // 如果需要新建的表已经存在
        try {
            if (admin.tableExists(tableName)) {
                System.out.println("表已经存在！");
            }
            // 如果需要新建的表不存在
            else {
                // 数据表描述对象
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                // 列族描述对象
                HColumnDescriptor family = new HColumnDescriptor("base");
                ;
                // 在数据表中新建一个列族
                hTableDescriptor.addFamily(family);
                // 新建数据表
                admin.createTable(hTableDescriptor);
                System.out.println("---------------创建表 END-----------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public void close() {

        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @RequestMapping(value = "/showAllTables")
    public void showAllTables() {
        init();
        try {
            HTableDescriptor[] hTableDescriptors = admin.listTables();
            for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
                System.out.println(hTableDescriptor.getTableName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    @RequestMapping(value = "/insert")
    public void insert() {
        init();
        try {
            Table table = connection.getTable(TableName.valueOf("aa"));
                Put put = new Put(Bytes.toBytes(3));
                put.addColumn(Bytes.toBytes("base"), Bytes.toBytes(3), Bytes.toBytes(3));
                table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    @RequestMapping(value = "/deleteTable")
    public void deleteTable() {
        init();
        TableName tableName = TableName.valueOf("bbb");
        try {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    @RequestMapping(value = "/deleteRow")
    public void deleteRow() {
        init();
        try {
            Table table = connection.getTable(TableName.valueOf("aa"));
            Delete delete = new Delete(Bytes.toBytes("1"));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    @RequestMapping(value = "/getData")
    // 根据rowkey查找数据
    public void getData() throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf("aa"));
        Get get = new Get(Bytes.toBytes("2"));
        // 获取指定列族数据
        // get.addFamily(Bytes.toBytes(colFamily));
        // 获取指定列数据
        // get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);
        showCell(result);
        table.close();
        close();
    }

    // 格式化输出
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        System.out.println(cells.length);
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + "Timetamp:" + cell.getTimestamp() + "column Family:" + new String(CellUtil.cloneFamily(cell)) + "row Name:" + new String(CellUtil.cloneQualifier(cell)) + "value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

    // 批量查找数据
    @RequestMapping(value = "/getAllData")
    public void scanData() throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf("aa"));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            showCell(result);
        }
        table.close();
        close();
    }


    private void init() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.113.231");
        // 设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
