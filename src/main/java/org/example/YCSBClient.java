package org.example;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import site.ycsb.*;


public class YCSBClient extends DB {

  private HTransaction htx;
  private  Connection conn;
  Admin admin;
  public YCSBClient() throws IOException{
    Configuration conf = new Configuration();
    conn = ConnectionFactory.createConnection(conf);
    admin = conn.getAdmin();
    htx = new HTransaction(conn, admin);
  }

  @Override
  public void init() throws DBException {
    System.out.println("USING THIS");
    TableName tableName = TableName.valueOf("NewTable");
    byte[] txnTimeStampFamily = Bytes.toBytes("family");

    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptorBuilder txnTimeStampFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder
            .newBuilder(Bytes.toBytes(ByteBuffer.wrap(txnTimeStampFamily)));
    tableDescriptorBuilder.setColumnFamily(txnTimeStampFamilyDescriptorBuilder.build());
    try {
      if (!this.admin.tableExists(tableName)) {
        TableDescriptor descriptor = tableDescriptorBuilder.build();
        admin.createTable(descriptor);
        System.out.println("Table created");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }


  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Get get = new Get(Bytes.toBytes(key));
    try {
      Table table_ = this.conn.getTable(TableName.valueOf(table));
      Result hbaseResult = table_.get(get);
      for (Cell cell : hbaseResult.listCells()) {
        String cf = Bytes.toString(CellUtil.cloneFamily(cell));
        String cq = Bytes.toString(CellUtil.cloneQualifier(cell));
        String field = cf + ":" + cq;
        byte[] value = CellUtil.cloneValue(cell);
        result.put(field, new ByteArrayByteIterator(value));
      }
      return Status.OK;
    } catch (IOException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }


  @Override
  public Status delete(String arg0, String arg1) {
    return Status.NOT_IMPLEMENTED;
  }



  @Override
  public Status insert(String arg0, String arg1, Map<String, ByteIterator> arg2) {
    for (Map.Entry<String, ByteIterator> entry : arg2.entrySet()) {
      String key = entry.getKey();
      ByteIterator value = entry.getValue();
      htx.newPut(arg0, arg1,  Bytes.toBytes("family"),  Bytes.toBytes(key), value.toArray());
      }
    boolean done = true;
    if(done){
      System.out.println("INSERTED ROW");
      return Status.OK;
    } else{
      return Status.ERROR;
    }

  }

  @Override
  public void cleanup() throws DBException {
    System.out.println(htx.columnToValue.size() + " Queries to commit");
    htx.commitQueries();
    htx.stop();
  }

  @Override
  public Status scan(String arg0, String arg1, int arg2, Set<String> arg3,
                     Vector<HashMap<String, ByteIterator>> arg4) {
    // TODO Auto-generated method stub
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String arg0, String arg1, Map<String, ByteIterator> arg2) {
    for (Map.Entry<String, ByteIterator> entry : arg2.entrySet()) {
      String key = entry.getKey();
      byte[] value = entry.getValue().toArray();
      int index = key.indexOf(":");
      if (index != -1) {
        String family = key.substring(0, index);
        String qualifier = key.substring(index + 1);

        htx.newPut(arg0, arg1,  Bytes.toBytes(family),  Bytes.toBytes(qualifier), value);
      }
    }
    return Status.OK;
  }

}


