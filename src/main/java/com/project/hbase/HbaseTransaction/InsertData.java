package com.project.hbase.HbaseTransaction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertData{

   public static void main(String[] args) throws IOException {

	   try {
		   Configuration conf = HBaseConfiguration.create();
		   Connection connection = ConnectionFactory.createConnection(conf);
		   Table table = connection.getTable(TableName.valueOf("test_table_example"));
		   if(table != null) {
			   System.out.println("hello");
		   }

		   table.close();
		   connection.close();
		 } finally {
		   
		}
}
}