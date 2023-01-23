package com.project.hbase.HbaseTransaction;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService.AsyncProcessor.put;
import org.apache.hadoop.hbase.util.Bytes;
// @Override
//  public Object run() throws Exception {
//   Put p = new Put(TEST_ROW);
//   p.addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1));
//   try(Connection conn = ConnectionFactory.createConnection(conf);
//     Table t = conn.getTable(TEST_TABLE)) {
//    t.checkAndMutate(TEST_ROW, TEST_FAMILY).qualifier(TEST_QUALIFIER)
//      .ifEquals(Bytes.toBytes("test_value")).thenPut(p);
//   }
//   return null;
//  }
// };

public class CreateAndPut{
    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        byte[] row = Bytes.toBytes("row32");
        byte[] cf = Bytes.toBytes("cf");
        byte[] cq = Bytes.toBytes("rq");
        byte[] value = Bytes.toBytes("NONE");
        Put put = new Put(row);
        put.addColumn(cf, cq, Bytes.toBytes("H2"));
        try{
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            if(admin.tableExists(TableName.valueOf("T2"))){
                Table table = connection.getTable(TableName.valueOf("T2"));
                table.put(new Put(row).addColumn(cf, cq,value));
                // table.checkAndMutate(row, cf).qualifier(cq).ifEquals(value).thenPut(put);        
                
            }
        } catch(Exception ex){
            System.out.println(ex.getMessage());
        }
        
    }
}
