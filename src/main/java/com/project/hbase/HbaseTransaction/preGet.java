package com.project.hbase.HbaseTransaction;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService.AsyncProcessor.put;
import org.apache.hadoop.hbase.util.Bytes;

public class preGet {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        try{
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            Table table = connection.getTable(TableName.valueOf("test"));
        } catch(Exception ex){
            ex.printStackTrace();
        }

    }
}
