package org.example;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;



public class TimestampUpdate implements Runnable{

  protected String transactionId;
  Table TransactionTable;
  byte[] transactionIdRow;
  byte[] transactionIdTimestampColumn;
  Put put;
  Connection connection;
  Admin admin;
  private volatile boolean stopTimestamp = false;

  public TimestampUpdate(Connection connection, Admin admin, String transactionId){
    this.transactionId = transactionId;
    this.transactionIdRow = Bytes.toBytes(transactionId.toString());
    this.transactionIdTimestampColumn = Bytes.toBytes("txnTimeStamp");
    this.put = new Put(this.transactionIdRow);
    this.connection = connection;
    this.admin = admin;
    try {
      this.TransactionTable = this.connection.getTable(TableName.valueOf("Monitor"));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("table cannot be updated: " + e.getMessage());
    }

  }

  @Override
  public void run() {
    byte[] transactionIdTimestampValue;
    while(!stopTimestamp){

      try {
        //byte[] transactionIdTimestampValue = Bytes.toBytes("ssss");
        transactionIdTimestampValue = Bytes.toBytes(Timestamp.valueOf(LocalDateTime.now()).toString());
        System.out.println(Timestamp.valueOf(LocalDateTime.now()).toString());
        this.TransactionTable.put(new Put(this.transactionIdRow).addColumn(this.transactionIdTimestampColumn, Bytes.toBytes("txnTime"), transactionIdTimestampValue));

        System.out.println("Timestamp updated");
        Thread.sleep(4000);
      } catch (InterruptedException | IOException e) {
        // TODO Auto-generated catch block
        System.out.println("TimestampUpdate interrupted");
        break;
      }
      if(Thread.currentThread().isInterrupted()){

      }
    }
  }

  public void shutdown(){

    stopTimestamp = true;
  }

}
