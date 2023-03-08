package org.example;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HTransaction {
  protected String transactionId;
  protected Connection connection;
  protected Admin admin;
  public boolean TxnProcess;
  Table TransactionTable;
  TimestampUpdate timestampUpdate;

  //need to update this map as tableName can already exist in hashmap which can lead to collision
  HashMap<String, NewTransactionsDetails> queries;

  HashMap<MapStage, byte[]> stageWrite;
  HashMap<String, UpdatesTransactionDetails> columnToValue;

  HashMap<UpdatesTransactionDetails, Get> returnGet;
  public ExecutorService executorService;
  public byte[] rowLockFamily = Bytes.toBytes("family");
  //public byte[] rowLockFamily = Bytes.toBytes("cf");
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
    this.columnToValue = new HashMap<String, UpdatesTransactionDetails>();
    this.stageWrite = new HashMap<MapStage, byte[]>();

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
      this.start();
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



  //Return the get command
  public Result getGetObj(String tableName, byte[] rowId){
    if (!rowExists(tableName, rowId)) {
      System.out.println("Row does not exists");
      return null;
    }

    System.out.println("Getting Result");
    boolean rowLockAcquired = acquireRowLock(tableName, rowId);
    Result result = new Result();
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
        result = table.get(new Get(rowId));
        // System.out.println(new String(result, StandardCharsets.UTF_8));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }
    return result;
  }


  public byte[] Read(String tableName, byte[] rowId, byte[] cf, byte[] cq) {
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
      System.out.println("old transaction ID" + new String(oldValue, StandardCharsets.UTF_8));
    } catch (Exception e) {
      System.out.println("Error in getOldValue");
    }
    return oldValue;
  }

  /**
   * method to acquire lock access on a row
   *
   * @param table
   * @param rowId
   * @return
   */
  private boolean acquireRowLock(String table, byte[] rowId) {
    System.out.println("acquiring row lock on " + table);
    boolean checkAndMutatePerformed = false;
    int retries = 1;

    try {
      Table newTable = this.connection.getTable(TableName.valueOf(table));
      Put put = new Put(rowId);
      put.addColumn(rowLockFamily, rowLockQuantifier, Bytes.toBytes(this.transactionId));
      CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(rowId).ifNotExists(rowLockFamily, rowLockQuantifier).build(put);

      while(true){
        CheckAndMutateResult result = newTable.checkAndMutate(checkAndMutate);
        if (result.isSuccess()) {
          checkAndMutatePerformed = true;
          break;
        }else{
          Thread.sleep(1000 * retries);
          retries +=1;
        }
        if (retries == 7){
          break;
        }
      }

    } catch (Exception e) {
      System.out.println("Failed on acquire row lock");
    }
    return checkAndMutatePerformed;
  }


  public boolean CheckTransactionTableForLock(byte[] txnId) {
    try {
      Table table = this.connection.getTable(TableName.valueOf("Monitor"));
      Result oldTxnTimestamp = table.get(new Get(txnId));
      Result newTxnTimestamp = table.get(new Get(Bytes.toBytes(this.transactionId)));

      long oldTimestamp = oldTxnTimestamp.rawCells()[0].getTimestamp();
      long newTimestamp = newTxnTimestamp.rawCells()[0].getTimestamp();
      Date oldRowTimestamp = new Date(oldTimestamp);

      Date newRowTimestamp = new Date(newTimestamp);
      if (Math.abs(newRowTimestamp.getTime() - oldRowTimestamp.getTime()) > 4000) {
        System.out.println((newRowTimestamp.getTime() - oldRowTimestamp.getTime())/1000);
        System.out.println("Difference between");

        return true;
      } else {
        System.out.println((newRowTimestamp.getTime() - oldRowTimestamp.getTime())/1000);
        System.out.println("Difference between");
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
  //Insert method is working properly
  public boolean Insert(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier, byte[] value){
    System.out.println("writing on row");
    boolean insertDone = false;
    boolean rowLockAcquired = false;
    boolean checkWriteLockAcquired = false;
    try{
      Table table = this.connection.getTable(TableName.valueOf(tableName));
      System.out.println("GOT TABLE");
      Get get = new Get(Bytes.toBytes(rowId));
      if(table.exists(get)){
        if(table.get(get).containsColumn(rowLockFamily, rowLockQuantifier)){
          String s = new String(table.get(get).getValue(rowLockFamily, rowLockQuantifier), StandardCharsets.UTF_8);
          if(s.equals(this.transactionId)){
            table.put(new Put(Bytes.toBytes(rowId)).addColumn(columnFamily, columnQuantifier, value));
            return true;
          }
        }
        rowLockAcquired = acquireRowLock(tableName, Bytes.toBytes(rowId));
        if(!rowLockAcquired){
          System.out.println("Row lock acquiring");
          byte[] oldrowId = getOldValue(tableName, Bytes.toBytes(rowId));
          checkWriteLockAcquired = CheckTransactionTableForLock(oldrowId);
          System.out.println("checking the result for write checktransactions " + checkWriteLockAcquired);
          if (!checkWriteLockAcquired) {
            //have to change this so that the constructor takes in the Txnid because the next time txnID will change
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
          insertDone = true;
        }

      } else{

        System.out.println("Inserting a new row");
        Put lockput = new Put(Bytes.toBytes(rowId));
        lockput.addColumn(rowLockFamily, rowLockQuantifier, Bytes.toBytes(this.transactionId));
        CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(Bytes.toBytes(rowId))
                  .ifNotExists(rowLockFamily, rowLockQuantifier).build(lockput);

        CheckAndMutateResult checkAndMutateResult = table.checkAndMutate(checkAndMutate);
        if(checkAndMutateResult.isSuccess()){
          table.put(new Put(Bytes.toBytes(rowId)).addColumn(columnFamily, columnQuantifier, value));
          insertDone = true;
          System.out.println("WRITTEN WITH NEW LOCK");
        }
      }

    } catch (Exception e){
      System.out.println("Problem with Insert method");
    }
    return insertDone;
  }

  // newPut is successfully putting all the transactions inside map
  public void newPut(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier, byte[] value) {
    UpdatesTransactionDetails newTransactionsDetails = new UpdatesTransactionDetails(tableName, rowId, columnFamily,
        columnQuantifier, value);

    this.columnToValue.put(tableName+rowId+new String(columnQuantifier, StandardCharsets.UTF_8), newTransactionsDetails);

  }
  public void orderTransactionCommit(){
    TreeMap<String, UpdatesTransactionDetails> sorted = new TreeMap<>();
    sorted.putAll(this.columnToValue);
  }

  //This method is not working properly.
  public boolean commitQueries() {
    boolean done = false;
    System.out.println("Starting to commit queries");
    System.out.println(columnToValue.size());
    for (Entry<String, UpdatesTransactionDetails> set : this.columnToValue.entrySet()) {
      try {
        System.out.println("Inserting data in " + set.getValue().tableName +" "+ set.getValue().rowId);
        done = Insert(set.getValue().tableName, set.getValue().rowId, set.getValue().columnFamily, set.getValue().columnQuantifier, set.getValue().value);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
    return done;
  }

  //Problem with this function
  public boolean transactionCommit(){
    boolean done = false;
    System.out.println("Transaction Commit started");
    System.out.println("Starting to acquire locks");


    for (Entry<String, UpdatesTransactionDetails> set : this.columnToValue.entrySet()) {
      System.out.println("New lock acquiring");
      boolean rowLockAcquire = false;
      boolean checkForRowLockWithMonitor = false;
      boolean rowExists = false;
      try {
        rowExists = rowExists(set.getValue().tableName, Bytes.toBytes(set.getValue().rowId));
        if(rowExists){
          Get get = new Get(Bytes.toBytes(set.getValue().rowId));
          Table table = this.connection.getTable(TableName.valueOf(set.getValue().tableName));
          if(table.get(get).containsColumn(rowLockFamily, rowLockQuantifier)){
            String s = new String(table.get(get).getValue(rowLockFamily, rowLockQuantifier), StandardCharsets.UTF_8);
            if(s.equals(this.transactionId)){
              done = true;
              continue;
            }
          }
          rowLockAcquire = acquireRowLock(set.getValue().tableName, Bytes.toBytes(set.getValue().rowId));
          if(!rowLockAcquire){
            System.out.println("Row currently used by another transaction. Checking with Monitor table");
            byte[] oldrowId = getOldValue(set.getValue().tableName, Bytes.toBytes(set.getValue().rowId));
            checkForRowLockWithMonitor = CheckTransactionTableForLock(oldrowId);
            if (!checkForRowLockWithMonitor) {
              System.out.println("Lock already acquired. Try again later");
              done = false;
              break;
            } else {
              done = ForceLockAccess(set.getValue().tableName, Bytes.toBytes(set.getValue().rowId), oldrowId, Bytes.toBytes(this.transactionId));
            }
          } else{
            done = true;
          }
        }
        else if(!rowExists){
          Table table = this.connection.getTable(TableName.valueOf(set.getValue().tableName));
          System.out.println("Inserting a new row");
          Put lockput = new Put(Bytes.toBytes(set.getValue().rowId));
          lockput.addColumn(rowLockFamily, rowLockQuantifier, Bytes.toBytes(this.transactionId));
          CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(Bytes.toBytes(set.getValue().rowId))
                  .ifNotExists(rowLockFamily, rowLockQuantifier).build(lockput);

          CheckAndMutateResult checkAndMutateResult = table.checkAndMutate(checkAndMutate);
          if(checkAndMutateResult.isSuccess()){
            done = true;
            System.out.println("New row created with lock");
          }
          else{
            done = false;
            break;
          }
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }


    //commit only if all lock acquired.
    if(done){
      System.out.println("All locks acquired starting to commit queries");
      done = commitQueries();
    }
    return done;
  }






   //tableName -> (put/get -> value);
//  public byte[] mapGet(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier) {
//    UpdatesTransactionDetails newTransactionsDetails = new UpdatesTransactionDetails(tableName, rowId, columnFamily,
//        columnQuantifier, Bytes.toBytes(value));
//    byte[] res = new byte[1];
//    try {
//      if (this.columnToValue.get(newTransactionsDetails) != null) {
//        System.out.println("Get value from Map");
//        System.out.println(new String(this.columnToValue.get(newTransactionsDetails), StandardCharsets.UTF_8));
//      } else {
//        System.out.println("Get value from table");
//        res = Read(tableName, Bytes.toBytes(rowId), columnFamily, columnQuantifier);
//        System.out.println(new String(res, StandardCharsets.UTF_8));
//      }
//
//    } catch (Exception e) {
//      System.out.println(e.getMessage());
//    }
//    return res;
//  }




  // ---------------------------------------------------------------- Server Stage Start ----------------------------------------------------------------
  @Deprecated
  public void Put(String tableName, String rowId, byte[] columnFamily, byte[] columnQuantifier, byte[] value) {
    try {
      Table table = this.connection.getTable(TableName.valueOf(tableName));
      NewTransactionsDetails newTransactionsDetails = new NewTransactionsDetails(rowId, columnFamily,
          columnQuantifier);

      //need to inverse this as key exists in the hashMap
      this.queries.put(tableName, newTransactionsDetails);
      System.out.println("put");
      table.put(new Put(Bytes.toBytes(rowId + "uncommitted")).addColumn(columnFamily, columnQuantifier, value));
      table.put(new Put(Bytes.toBytes(rowId)).addColumn(rowLockFamily, rowLockQuantifier,
          Bytes.toBytes(this.transactionId)));
    } catch (IOException ex) {
      System.out.println(ex.getMessage());

    }
  }

  @Deprecated
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
// ---------------------------------------------------------------- Server stage Finished ----------------------------------------------------------------

  public static void main(String[] args) {
//    // try {
//    // Configuration conf = HBaseConfiguration.create();
//    // Connection conn = ConnectionFactory.createConnection(conf);
//    // Admin admin = conn.getAdmin();
//    // HTransaction htx = new HTransaction(conn, admin);
//    // htx.start();
//    // System.out.println(htx.transactionId);
//    // byte[] row = Bytes.toBytes("Arlo");
//    // byte[] result = htx.GetRow("T2", row, Bytes.toBytes("cf"),
//    // Bytes.toBytes("cf"));
//    // System.out.println(new String(result, StandardCharsets.UTF_8));
//    // // htx.Put("T2", "Arlo", Bytes.toBytes("cf"), Bytes.toBytes("cf"),
//    // // Bytes.toBytes("Cat"));
//    // // htx.commit();
//
//    // htx.stop();
//    // } catch (IOException ex) {
//    // }
    try {
      Configuration conf = new Configuration();
      Connection conn = ConnectionFactory.createConnection(conf);
      Admin admin = conn.getAdmin();
      HTransaction htx = new HTransaction(conn, admin);
      htx.newPut("T2", "row2", Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("new caitlin value"));
      Thread.sleep(1000);
      htx.newPut("T2", "row1", Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("new Cat Value"));
      //    Thread.sleep(1000);
      //    htx.newPut("T2", "CATS", Bytes.toBytes("cf"), Bytes.toBytes("cf"), Bytes.toBytes("is best"));
      //    Thread.sleep(1000);
      //    htx.mapGet("T2", "Cat4", Bytes.toBytes("cf"), Bytes.toBytes("cf"));
      //    Thread.sleep(1000);
      //    htx.mapGet("T2", "CaitlinI", Bytes.toBytes("cf"), Bytes.toBytes("cf"));
      Thread.sleep(1000);
      htx.transactionCommit();
      Thread.sleep(1000);
      htx.stop();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
//    // try{
//    // Configuration conf = HBaseConfiguration.create();
//    // Connection conn = ConnectionFactory.createConnection(conf);
//    // Admin admin = conn.getAdmin();
//    // HTransaction txn = new HTransaction(conn, admin);
//    // txn.Get("test", Bytes.toBytes("row1"));
//
//    // } catch (Exception e){
//
//    // }
//    // try {
//    // Configuration conf = HBaseConfiguration.create();
//    // Connection connection = ConnectionFactory.createConnection(conf);
//    // Admin admin = connection.getAdmin();
//    // HTransaction txn = new HTransaction(connection, admin);
//    // txn.start();
//    // TimeUnit.SECONDS.sleep(1);
//    // txn.print("h1");
//    // TimeUnit.SECONDS.sleep(1);
//    // txn.print("h2");
//    // TimeUnit.SECONDS.sleep(1);
//    // txn.print("h3");
//    // TimeUnit.SECONDS.sleep(1);
//    // txn.print("h4");
//    // TimeUnit.SECONDS.sleep(1);
//    // txn.print("h5");
//    // TimeUnit.SECONDS.sleep(8);
//    // txn.stop();
//    // System.out.println("1");
//    // } catch (Exception e) {
//    // System.out.println("Main error: ");
//    // }
  }

}
