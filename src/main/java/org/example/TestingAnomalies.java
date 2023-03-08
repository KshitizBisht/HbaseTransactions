package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class TestingAnomalies {
    public static void main(String[] args) {
        try{
            Configuration conf = new Configuration();
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();


            Thread thread1 = new Thread() {
                public void run() {
                    HTransaction htx = new HTransaction(conn, admin);
                    htx.newPut("T2", "row1", Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("value from txn1"));
                    htx.newPut("T2", "row2", Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("value from txn1"));
                    htx.transactionCommit();
                    htx.stop();
                }
            };

            Thread thread2 = new Thread() {
                public void run() {
                    HTransaction htx = new HTransaction(conn, admin);
                    htx.newPut("T2", "row2", Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("value from txn2"));
                    htx.newPut("T2", "row3", Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("value from txn2"));
                    htx.transactionCommit();
                    htx.stop();
                }
            };

            thread1.start();
            thread2.start();


        } catch (Exception e){

        }
    }
}
