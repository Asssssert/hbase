package com.assssert.hbase;

import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@SpringBootApplication
public class HbaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(HbaseApplication.class, args);
    }

    @RequestMapping("/test")
    public void test() {
        Connection connection ;
        Admin admin = null;
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.113.231");
        // 设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin= connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 数据表表名
        String tableNameString = "bbb";

        // 新建一个数据表表名对象
        TableName tableName = TableName.valueOf(tableNameString);

        // 如果需要新建的表已经存在
        try {
            if(admin.tableExists(tableName)){

                System.out.println("表已经存在！");
            }
            // 如果需要新建的表不存在
            else{

                // 数据表描述对象
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

                // 列族描述对象
                HColumnDescriptor family= new HColumnDescriptor("base");;

                // 在数据表中新建一个列族
                hTableDescriptor.addFamily(family);

                // 新建数据表
                admin.createTable(hTableDescriptor);
                System.out.println("---------------创建表 END-----------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
