package com.project.hbase.HbaseTransaction;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class checkFamily{
    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        byte[] row = Bytes.toBytes("row1");
        byte[] cf = Bytes.toBytes("Lock");
        byte[] cq = Bytes.toBytes("write Lock");
        byte[] value = Bytes.toBytes("value");
        try{
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            Table table = connection.getTable(TableName.valueOf("test"));
         
            System.out.println(table.get(new Get(row)).containsColumn(cf, cq));

        } catch (Exception exception){
            exception.printStackTrace();
        }
    }
}