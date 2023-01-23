package com.project.hbase.HbaseTransaction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService.AsyncProcessor.put;
import org.apache.hadoop.hbase.util.Bytes;

import com.nimbusds.jose.util.StandardCharset;

public class getData {
    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        byte[] row = Bytes.toBytes("row212");
        byte[] cf = Bytes.toBytes("cf");
        byte[] cq = Bytes.toBytes("cq");
        byte[] value = Bytes.toBytes("value");
        try{
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            if(admin.tableExists(TableName.valueOf("T2"))){
                Table table = connection.getTable(TableName.valueOf("T2"));
                Result result = table.get(new Get(row));
                // byte[] x = result.listCells().get(0).getFamilyArray();
                // byte[] res = Arrays.copyOfRange(x, result.listCells().get(0).getRowOffset(),result.listCells().get(0).getRowOffset()+result.listCells().get(0).getRowLength());
                // byte[] fam = Arrays.copyOfRange(x, result.listCells().get(0).getFamilyOffset(),result.listCells().get(0).getFamilyOffset()+result.listCells().get(0).getFamilyLength());
                // byte[] quan = Arrays.copyOfRange(x, result.listCells().get(0).getQualifierOffset(),result.listCells().get(0).getQualifierOffset()+result.listCells().get(0).getQualifierLength());
                // byte[] val = Arrays.copyOfRange(x, result.listCells().get(0).getValueOffset(),result.listCells().get(0).getValueOffset()+result.listCells().get(0).getValueLength());
                // System.out.println(new String(res, StandardCharsets.UTF_8));
                // System.out.println(new String(fam, StandardCharsets.UTF_8));
                // System.out.println(new String(quan, StandardCharsets.UTF_8));
                // System.out.println(new String(val, StandardCharsets.UTF_8));
                // System.out.println(result.listCells().get(0).getRowLength());
                // System.out.println(new String(table.get(new Get(row)).getValue(cf, cq)));
                Put put = new Put(row);
                put.addColumn(cf, cq, 0000, value);
                table.put(put);
                
                
                //byte[] result = table.get(new Get(row)).getValue(cf, cq);
                // System.out.println(new String(result, StandardCharset.UTF_8));
            }

        } catch (Exception ex){
            System.out.println(ex.getMessage());
        }
    }
    
}
