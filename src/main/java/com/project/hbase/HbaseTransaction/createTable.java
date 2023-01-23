package com.project.hbase.HbaseTransaction;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class createTable{
    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        try{
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            byte[] family = Bytes.toBytes("cf");
            byte[] Lfamily = Bytes.toBytes("rf");
            TableName tname = TableName.valueOf("T3");
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tname);
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.
            newBuilder(Bytes.toBytes(ByteBuffer.wrap(family)));
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            if(!admin.tableExists(tname)){
                TableDescriptor descriptor = tableDescriptorBuilder.build();
                admin.createTable(descriptor);
                System.out.println("CREAted");
                
            }
            TableName[] tablenames = admin.listTableNames();
            for (int i = 0; i<tablenames.length; i++){
                System.out.println(tablenames[i].getQualifierAsString());
            }
            


        } catch(Exception e){
            System.out.println(e.getMessage().toString());
        }
         finally{
            System.out.println("DONE");
        }
       
    }
}