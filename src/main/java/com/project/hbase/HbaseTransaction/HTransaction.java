package com.project.hbase.HbaseTransaction;

import java.nio.ByteBuffer;
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
    HashMap<UpdatesTransactionDetails, byte[]> newQueries;
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
        // this.transactionId = "SSS";
        this.connection = connection;
        this.admin = admin;
        this.TxnProcess = true;
        this.queries = new HashMap<String, NewTransactionsDetails>();
        this.newQueries = new HashMap<UpdatesTransactionDetails, byte[]>();

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

    /**
     * Initial draft version for get Function
     * 
     * @param table
     * @param rowId
     */
    public void Get(String table, byte[] rowId) {
        try {
            Table newTable = this.connection.getTable(TableName.valueOf(table));
            Put put = new Put(rowId);
            put.addColumn(rowLockFamily, rowLockQuantifier, Bytes.toBytes(this.transactionId));
            CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(rowId)
                    .ifNotExists(rowLockFamily, rowLockQuantifier).build(put);

            CheckAndMutateResult result = newTable.checkAndMutate(checkAndMutate);
            System.out.println(result.isSuccess());

        } catch (Exception e) {
            System.out.println("Error in read");
        }

    }

    public byte[] getOldValue(String tableName, byte[] rowId, byte[] cf, byte[] cq) {
        byte[] oldValue = new byte[1];
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            oldValue = table.get(new Get(rowId)).getValue(cf, cq);
        } catch (Exception e) {
            System.out.println("Error in getOldValue");
        }
        return oldValue;
    }

    public byte[] GetRow(String tableName, byte[] rowId, byte[] cf, byte[] cq) {
        // CALL THE acquireRowLock() method
        boolean rowLockAcquired = acquireRowLock(tableName, rowId);
        byte[] result = new byte[1];
        boolean checkTransaction;

        if (!rowLockAcquired) {
            byte[] oldrowId = getOldValue(tableName, rowId, cf, cq);
            checkTransaction = CheckTransactionTableForLock(oldrowId);
            if (!checkTransaction) {
                System.out.println("Lock already acquired. Try again");
            } else {
                rowLockAcquired = ForceLockAccess(tableName, rowId, oldrowId, Bytes.toBytes(this.transactionId));
            }
        }
        if (rowLockAcquired) {
            try {
                Table table = this.connection.getTable(TableName.valueOf(tableName));
                result = table.get(new Get(rowId)).getValue(cf, cq);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        return result;
    }

    /**
     * method to acuire lock acess on a row
     * 
     * @param table
     * @param rowId
     * @return
     */
    private boolean acquireRowLock(String table, byte[] rowId) {

        boolean checkAndMutatePerformed = false;
        int exponentialBackOff = 2;
        int numberOfTriesToAcquireLock = 0;

        try {
            Table newTable = this.connection.getTable(TableName.valueOf(table));
            Put put = new Put(rowId);
            put.addColumn(rowLockFamily, rowLockQuantifier, Bytes.toBytes(this.transactionId));

            while (true) {

                CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(rowId)
                        .ifNotExists(rowLockFamily, rowLockQuantifier).build(put);
                CheckAndMutateResult result = newTable.checkAndMutate(checkAndMutate);
                if (result.isSuccess()) {
                    checkAndMutatePerformed = true;
                    break;
                }

                TimeUnit.SECONDS.sleep(exponentialBackOff);
                exponentialBackOff = exponentialBackOff * 2;
                numberOfTriesToAcquireLock += 1;

                if (numberOfTriesToAcquireLock == 5) {
                    break;
                }
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
                    forceLockAcessSuccess = true;
                    break;
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return forceLockAcessSuccess;
    }

    // cannot do this as there may be transaction with same id but different column
    // names
    // public byte[] getBytes(byte[] bytes, int offset, int length){
    // return Arrays.copyOfRange(bytes, offset, offset+length);
    // }

    // public void commit(){
    // Table table;
    // for (Entry<String, String> set :
    // this.queries.entrySet()) {
    // try {
    // table = this.connection.getTable(TableName.valueOf(set.getKey()));
    // Result result = table.get(new
    // Get(Bytes.toBytes(set.getValue()+"UncommittedRow")));
    // byte[] resultBytes = result.listCells().get(0).getFamilyArray();

    // byte[] row = Bytes.toBytes(set.getValue());
    // byte[] columnFamily = getBytes(resultBytes,
    // result.listCells().get(0).getFamilyOffset(),
    // result.listCells().get(0).getFamilyLength());
    // byte[] columnQuantifier = getBytes(resultBytes,
    // result.listCells().get(0).getQualifierOffset(),
    // result.listCells().get(0).getQualifierLength());
    // byte[] value = getBytes(resultBytes,
    // result.listCells().get(0).getValueOffset(),
    // result.listCells().get(0).getValueLength());
    // table.put(new Put(new Put(row).addColumn(columnFamily, columnQuantifier,
    // value)));
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }

    // }
    // }

    public void commit() {
        Table table;
        for (Entry<String, NewTransactionsDetails> set : this.queries.entrySet()) {
            try {
                table = this.connection.getTable(TableName.valueOf(set.getKey()));
                byte[] row = Bytes.toBytes(set.getValue().getRowId() + "uncommitted");
                byte[] result = table.get(new Get(row)).getValue(set.getValue().getCf(), set.getValue().getCq());
                byte[] newRowId = Bytes.toBytes(set.getValue().getRowId());
                table.put(new Put(newRowId).addColumn(set.getValue().getCf(), set.getValue().getCq(), result));
            } catch (Exception e) {

            }
        }
    }

    public void Put(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier, byte[] value) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            // boolean checkRowExists = table.get(new Get(rowId)).isEmpty();
            // if(checkRowExists){

            NewTransactionsDetails newTransactionsDetails = new NewTransactionsDetails(rowId, columnFamily,
                    columnQuantifier);
            this.queries.put(tableName, newTransactionsDetails);
            System.out.println("put");
            table.put(new Put(Bytes.toBytes(rowId + "uncommitted")).addColumn(columnFamily, columnQuantifier, value));
            table.put(new Put(Bytes.toBytes(rowId)).addColumn(rowLockFamily, rowLockQuantifier, Bytes.toBytes(rowId)));

            // }

        } catch (IOException ex) {
            System.out.println(ex.getMessage());

        }
    }

    public void newPut(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier, byte[] value) {
        try {
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            // boolean checkRowExists = table.get(new Get(rowId)).isEmpty();
            // if(checkRowExists){

            UpdatesTransactionDetails newTransactionsDetails = new UpdatesTransactionDetails(rowId, columnFamily,
                    columnQuantifier);
            this.newQueries.put(newTransactionsDetails, value);
            System.out.println("New put");
            table.put(new Put(Bytes.toBytes(rowId + "uncommitted")).addColumn(columnFamily, columnQuantifier, value));
            table.put(new Put(Bytes.toBytes(rowId)).addColumn(rowLockFamily, rowLockQuantifier, Bytes.toBytes(rowId)));

            // }

        } catch (IOException ex) {
            System.out.println(ex.getMessage());

        }
    }

    public byte[] getNewRead(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier) {
        UpdatesTransactionDetails newRead = new UpdatesTransactionDetails(rowId, columnFamily, columnQuantifier);
        if (this.newQueries.containsKey(newRead)) {
            return this.newQueries.get(newRead);

        } else {
            try {
                Table table = this.connection.getTable(TableName.valueOf(tableName));

            } catch (Exception e) {
                System.out.println("Error in getting table for new read");
            }

        }

    }

    public static void main(String[] args) {
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            HTransaction htx = new HTransaction(conn, admin);
            byte[] row = Bytes.toBytes("row");
            htx.Put("T2", "row212", Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("hello"));
            htx.commit();
        } catch (IOException ex) {
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
