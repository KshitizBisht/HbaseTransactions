package com.project.hbase.HbaseTransaction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
import java.util.HashMap;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class HTransaction {
    protected String transactionId;
    protected Connection connection;
    protected Admin admin;
    public boolean TxnProcess;
    Table TransactionTable;
    TimestampUpdate timestampUpdate;
    HashMap<String, NewTransactionsDetails> queries;
    HashMap<UpdatesTransactionDetails, byte[]> columnToValue;
    public ExecutorService executorService;
    public byte[] rowLockFamily = Bytes.toBytes("cf");
    public byte[] rowLockQuantifier = Bytes.toBytes("rq");

    /**
     * Contructor method to initialize the transaction
     * creates a Transaction Monitor table if not already created
     * 
     * @param connection
     * @param admin
     */
    public HTransaction(Connection connection, Admin admin) {
        this.transactionId = UUID.randomUUID().toString();
        this.connection = connection;
        this.admin = admin;
        this.TxnProcess = true;
        this.queries = new HashMap<String, NewTransactionsDetails>();
        this.columnToValue = new HashMap<UpdatesTransactionDetails, byte[]>();

        try {
            if (!admin.tableExists(TableName.valueOf("Monitor"))) {
                createTransactionTable();
                this.TransactionTable = connection.getTable(TableName.valueOf("Monitor"));
                if (this.TransactionTable == null) {
                    System.out.println("Not created");
                }
                System.out.println("Transaction Table started successfully");
            } else {
                System.out.println("Transaction Table already created");
            }
        } catch (Exception e) {
            System.out.println("Error in HTransactionTable constructor: " + e.getMessage());
        } finally {

        }

    }

    /*
     * Helper method to create a new transaction table for monitoring
     */
    private void createTransactionTable() {

        try {
            // byte[] txnIDFamily = Bytes.toBytes("txnID");
            TableName tableName = TableName.valueOf("Monitor");
            byte[] txnTimeStampFamily = Bytes.toBytes("txnTimeStamp");

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            // ColumnFamilyDescriptorBuilder txnIDFamilyDescriptorBuilder =
            // ColumnFamilyDescriptorBuilder.
            // newBuilder(Bytes.toBytes(ByteBuffer.wrap(txnIDFamily)));
            ColumnFamilyDescriptorBuilder txnTimeStampFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(ByteBuffer.wrap(txnTimeStampFamily)));

            // tableDescriptorBuilder.setColumnFamily(txnIDFamilyDescriptorBuilder.build());
            tableDescriptorBuilder.setColumnFamily(txnTimeStampFamilyDescriptorBuilder.build());
            if (!this.admin.tableExists(tableName)) {
                TableDescriptor descriptor = tableDescriptorBuilder.build();
                admin.createTable(descriptor);
                System.out.println("Transaction Table created");
            }

        } catch (Exception ex) {
            System.out.println("Error in table creation" + ex.getMessage());
        }
    }

    /**
     * method to start the background process of updating the timestamp of the
     * transaction
     * Necessary to show that the transaction is in progress
     */
    public void start() {
        this.timestampUpdate = new TimestampUpdate(this.connection, this.admin, this.transactionId);
        this.executorService = Executors.newFixedThreadPool(2);
        this.executorService.submit(timestampUpdate);
    }

    /**
     * Method to stop the transaction process of updating timestamp
     * necessary to stop the transaction
     */
    public void stop() {
        this.executorService.shutdownNow();

    }

    /**
     * method for checking will be deleted later
     */
    public void print(String message) {
        System.out.println(message);
    }

    // ---------------------------------------------------------------- Read Starts ----------------------------------------------------------------

    public boolean rowExists(String tableName, byte[] rowId) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            return table.exists(new Get(rowId));
        } catch (Exception e) {

        }
        return false;
    }

    public byte[] GetRow(String tableName, byte[] rowId, byte[] cf, byte[] cq) {
        if (!rowExists(tableName, rowId)) {
            System.out.println("Row does not exists");
            return new byte[1];
        }

        System.out.println("Getting row");
        boolean rowLockAcquired = acquireRowLock(tableName, rowId);
        byte[] result = new byte[1];
        boolean checkTransaction;

        if (!rowLockAcquired) {
            byte[] oldrowId = getOldValue(tableName, rowId);
            checkTransaction = CheckTransactionTableForLock(oldrowId);
            System.out.println("checking the result for checktransactions " + checkTransaction);
            if (!checkTransaction) {
                System.out.println("Lock already acquired. Try again");
            } else {
                rowLockAcquired = ForceLockAccess(tableName, rowId, oldrowId, Bytes.toBytes(this.transactionId));
            }
        }
        if (rowLockAcquired) {
            try {
                System.out.println("row lock acquired" + rowLockAcquired);
                Table table = this.connection.getTable(TableName.valueOf(tableName));
                System.out.println("Got the table");
                result = table.get(new Get(rowId)).getValue(cf, cq);
                // System.out.println(new String(result, StandardCharsets.UTF_8));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        return result;
    }

    /**
     * This method is called to get the id of the transaction holding the lock.
     * 
     * @param tableName
     * @param rowId
     * @return
     */
    public byte[] getOldValue(String tableName, byte[] rowId) {
        byte[] oldValue = new byte[1];
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            oldValue = table.get(new Get(rowId)).getValue(rowLockFamily, rowLockQuantifier);
            System.out.println(new String(oldValue, StandardCharsets.UTF_8));
        } catch (Exception e) {
            System.out.println("Error in getOldValue");
        }
        return oldValue;
    }

    /**
     * method to acuire lock acess on a row
     * 
     * @param table
     * @param rowId
     * @return
     */
    private boolean acquireRowLock(String table, byte[] rowId) {
        System.out.println("acquiring row lock on " + table);
        boolean checkAndMutatePerformed = false;

        try {
            Table newTable = this.connection.getTable(TableName.valueOf(table));
            Put put = new Put(rowId);
            put.addColumn(rowLockFamily, rowLockQuantifier, Bytes.toBytes(this.transactionId));
            CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(rowId)
                    .ifNotExists(rowLockFamily, rowLockQuantifier).build(put);
            CheckAndMutateResult result = newTable.checkAndMutate(checkAndMutate);
            if (result.isSuccess()) {
                checkAndMutatePerformed = true;
            }
        } catch (Exception e) {
            System.out.println("Error in acquiring Locks");
        }
        return checkAndMutatePerformed;
    }

    /**
     * @param txnId
     * @param tableName
     * @param rowId
     */
    public boolean CheckTransactionTableForLock(byte[] txnId) {
        try {
            Table table = this.connection.getTable(TableName.valueOf("Monitor"));
            Result oldTxnTimestamp = table.get(new Get(txnId));
            Result newTxnTimestamp = table.get(new Get(Bytes.toBytes(this.transactionId)));

            long oldTimestamp = oldTxnTimestamp.rawCells()[0].getTimestamp();
            long newTimestamp = newTxnTimestamp.rawCells()[0].getTimestamp();
            if (Math.abs(newTimestamp - oldTimestamp) > 9) {
                System.out.println(newTimestamp - oldTimestamp);

                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return false;
    }

    // do this in loop also
    // while(true){
    // read old value
    // compare and swap from old to new
    // if success break;}

    /**
     * 
     * @param tableName
     * @param rowId
     * @param oldtxn
     * @param newtxn
     * @return
     */
    public boolean ForceLockAccess(String tableName, byte[] rowId, byte[] oldtxn, byte[] newtxn) {
        boolean forceLockAcessSuccess = false;
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowId);
            put.addColumn(rowLockFamily, rowLockQuantifier, newtxn);
            while (true) {
                CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(rowId)
                        .ifEquals(rowLockFamily, rowLockQuantifier, oldtxn).build(put);

                CheckAndMutateResult checkAndMutateResult = table.checkAndMutate(checkAndMutate);
                if (checkAndMutateResult.isSuccess()) {
                    System.out.println("force lock access");
                    forceLockAcessSuccess = true;
                    break;
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return forceLockAcessSuccess;
    }

    // ---------------------------------------------------------------- Read Finished ----------------------------------------------------------------
    public void writePut(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier, byte[] value){
        System.out.println("writing on row");
        boolean rowLockAcquired = false;
        boolean checkWriteLockAcquired = false;
        try{
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            if(table.exists(new Get(Bytes.toBytes(rowId)))){
                rowLockAcquired = acquireRowLock(tableName, Bytes.toBytes(rowId));
                if(!rowLockAcquired){
                    byte[] oldrowId = getOldValue(tableName, Bytes.toBytes(rowId));
                    checkWriteLockAcquired = CheckTransactionTableForLock(oldrowId);
                    System.out.println("checking the result for write checktransactions " + checkWriteLockAcquired);
                    if (!checkWriteLockAcquired) {
                        System.out.println("Lock already acquired. Try again");
                    } else {
                        rowLockAcquired = ForceLockAccess(tableName, Bytes.toBytes(rowId), oldrowId, Bytes.toBytes(this.transactionId));
                    }
                }
                if (rowLockAcquired) {
                        System.out.println("write row lock acquired" + rowLockAcquired);
                        System.out.println("Got the table");
                        table.put(new Put(Bytes.toBytes(rowId)).addColumn(columnFamily, columnQuantifier, value));
                        System.out.println("Successfully written in row");
                    }

            } else{
                // Is lock necessary if adding new rowId?
                //table.put(new Put(Bytes.toBytes(rowId)).addColumn(rowLockFamily, rowLockQuantifier, Bytes.toBytes(this.transactionId)));
                table.put(new Put(Bytes.toBytes(rowId)).addColumn(columnFamily, columnQuantifier, value));
            }
            
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void Put(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier, byte[] value) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            NewTransactionsDetails newTransactionsDetails = new NewTransactionsDetails(rowId, columnFamily,
                    columnQuantifier);
            this.queries.put(tableName, newTransactionsDetails);
            System.out.println("put");
            table.put(new Put(Bytes.toBytes(rowId + "uncommitted")).addColumn(columnFamily, columnQuantifier, value));
            table.put(new Put(Bytes.toBytes(rowId)).addColumn(rowLockFamily, rowLockQuantifier,
                    Bytes.toBytes(this.transactionId)));
        } catch (IOException ex) {
            System.out.println(ex.getMessage());

        }
    }

    public void commit() {
        Table table;
        for (Entry<String, NewTransactionsDetails> set : this.queries.entrySet()) {
            try {
                table = this.connection.getTable(TableName.valueOf(set.getKey()));
                byte[] row = Bytes.toBytes(set.getValue().getRowId() + "uncommitted");
                byte[] result = table.get(new Get(row)).getValue(set.getValue().getCf(), set.getValue().getCq());
                byte[] newRowId = Bytes.toBytes(set.getValue().getRowId());
                //use the custom put method here.
                //table.put(new Put(newRowId).addColumn(set.getValue().getCf(), set.getValue().getCq(), result));
            } catch (Exception e) {

            }
        }
    }

    // table -> rowId -> columnFamily and columnQuantifier -> value
    public void newPut(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier, byte[] value) {
        UpdatesTransactionDetails newTransactionsDetails = new UpdatesTransactionDetails(tableName, rowId, columnFamily,
                columnQuantifier);
        this.columnToValue.put(newTransactionsDetails, value);
        System.out.println("New put");

    }

    public void mapGet(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier) {
        UpdatesTransactionDetails newTransactionsDetails = new UpdatesTransactionDetails(tableName, rowId, columnFamily,
                columnQuantifier);
        try {
            if (this.columnToValue.get(newTransactionsDetails) != null) {
                System.out.println("Get value from Map");
                System.out.println(new String(this.columnToValue.get(newTransactionsDetails), StandardCharsets.UTF_8));
            } else {
                System.out.println("Get value from table");
                byte[] res = GetRow(tableName, Bytes.toBytes(rowId), columnFamily, columnQuantifier);
                System.out.println(new String(res, StandardCharsets.UTF_8));
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void commitQueries() {
        Table table;
        for (Entry<UpdatesTransactionDetails, byte[]> set : this.columnToValue.entrySet()) {
            try {
                table = this.connection.getTable(TableName.valueOf(set.getKey().tableName));
                // call the custom write method here.
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        // try {
        // Configuration conf = HBaseConfiguration.create();
        // Connection conn = ConnectionFactory.createConnection(conf);
        // Admin admin = conn.getAdmin();
        // HTransaction htx = new HTransaction(conn, admin);
        // htx.start();
        // System.out.println(htx.transactionId);
        // byte[] row = Bytes.toBytes("Arlo");
        // byte[] result = htx.GetRow("T2", row, Bytes.toBytes("cf"),
        // Bytes.toBytes("cf"));
        // System.out.println(new String(result, StandardCharsets.UTF_8));
        // // htx.Put("T2", "Arlo", Bytes.toBytes("cf"), Bytes.toBytes("cf"),
        // // Bytes.toBytes("Cat"));
        // // htx.commit();

        // htx.stop();
        // } catch (IOException ex) {
        // }
        try {
            Configuration conf = new Configuration();
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            HTransaction htx = new HTransaction(conn, admin);
            htx.newPut("T2", "Caitlin", Bytes.toBytes("cf"), Bytes.toBytes("cf"), Bytes.toBytes("This is new Value"));
            htx.mapGet("T2", "Arlo", Bytes.toBytes("cf"), Bytes.toBytes("cf"));
            htx.mapGet("T2", "Caitlin", Bytes.toBytes("cf"), Bytes.toBytes("cf"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        // try{
        // Configuration conf = HBaseConfiguration.create();
        // Connection conn = ConnectionFactory.createConnection(conf);
        // Admin admin = conn.getAdmin();
        // HTransaction txn = new HTransaction(conn, admin);
        // txn.Get("test", Bytes.toBytes("row1"));

        // } catch (Exception e){

        // }
        // try {
        // Configuration conf = HBaseConfiguration.create();
        // Connection connection = ConnectionFactory.createConnection(conf);
        // Admin admin = connection.getAdmin();
        // HTransaction txn = new HTransaction(connection, admin);
        // txn.start();
        // TimeUnit.SECONDS.sleep(1);
        // txn.print("h1");
        // TimeUnit.SECONDS.sleep(1);
        // txn.print("h2");
        // TimeUnit.SECONDS.sleep(1);
        // txn.print("h3");
        // TimeUnit.SECONDS.sleep(1);
        // txn.print("h4");
        // TimeUnit.SECONDS.sleep(1);
        // txn.print("h5");
        // TimeUnit.SECONDS.sleep(8);
        // txn.stop();
        // System.out.println("1");
        // } catch (Exception e) {
        // System.out.println("Main error: ");
        // }
    }

}
