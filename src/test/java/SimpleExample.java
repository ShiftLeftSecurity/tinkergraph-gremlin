//import java.io.File;
//
//import com.sleepycat.bind.EntryBinding;
//import com.sleepycat.bind.serial.ClassCatalog;
//import com.sleepycat.bind.serial.SerialBinding;
//import com.sleepycat.bind.serial.StoredClassCatalog;
//import com.sleepycat.bind.tuple.*;
//import com.sleepycat.collections.StoredKeySet;
//import com.sleepycat.je.Cursor;
//import com.sleepycat.je.Database;
//import com.sleepycat.je.DatabaseConfig;
//import com.sleepycat.je.DatabaseEntry;
//import com.sleepycat.je.DatabaseException;
//import com.sleepycat.je.Environment;
//import com.sleepycat.je.EnvironmentConfig;
//import com.sleepycat.je.LockMode;
//import com.sleepycat.je.OperationStatus;
//import com.sleepycat.je.Transaction;
//
//class SimpleExample {
//  private static int numRecords = 3;   // num records to insert or retrieve
//  private static int offset = 0;       // where we want to start inserting
//  private static File envDir = new File("bdbenv");
//
//  public static void main(String args[]) {
////    runCursors(true);
//    runCursors(false);
//
//    //    runKeyset(true);
////    runKeyset(false);
//
//    //    runTuple(true);
////    runTuple(false);
//
////   runSimple(true);
////    runSimple(false);
//  }
//
//  public static void runCursors(boolean doInsert) throws DatabaseException {
//    EnvironmentConfig envConfig = new EnvironmentConfig();
//    envConfig.setTransactional(true);
//    envConfig.setAllowCreate(true);
//    Environment environment = new Environment(envDir, envConfig);
//
//    Transaction txn = environment.beginTransaction(null, null);
//    DatabaseConfig dbConfig = new DatabaseConfig();
//    dbConfig.setTransactional(true);
//    dbConfig.setAllowCreate(true);
//    dbConfig.setSortedDuplicates(true);
//    Database db = environment.openDatabase(txn,"cursors", dbConfig);
//    txn.commit();
//
//    DatabaseEntry keyEntry = new DatabaseEntry();
//    DatabaseEntry dataEntry = new DatabaseEntry();
//
//    if (doInsert) {
//      for (int i = offset; i < numRecords + offset; i++) {
//        txn = environment.beginTransaction(null, null);
//        IntegerBinding.intToEntry(i, keyEntry);
//        IntegerBinding.intToEntry(i+10, dataEntry);
//        OperationStatus status = db.put(txn, keyEntry, dataEntry);
//
//        if (status != OperationStatus.SUCCESS) {
//          throw new RuntimeException("Data insertion got status " + status);
//        }
//        txn.commit();
//      }
//    } else {
//      IntegerBinding.intToEntry(1, keyEntry);
////      System.out.println(dataEntry); // null
////      System.out.println(db.get(null, keyEntry, dataEntry, null));
////      System.out.println(IntegerBinding.entryToInt(dataEntry));
//
//      // fetch all
//      Cursor cursor = db.openCursor(null, null);
////      while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
////        System.out.println("key=" + IntegerBinding.entryToInt(keyEntry) + " data=" + IntegerBinding.entryToInt(dataEntry));
////      }
//
//      System.out.println(cursor.getSearchKey(keyEntry, dataEntry, LockMode.DEFAULT));
//      System.out.println(cursor.count());
////      System.out.println(cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT));
//      System.out.println("key=" + IntegerBinding.entryToInt(keyEntry) + " data=" + IntegerBinding.entryToInt(dataEntry));
//
//      cursor.close();
//    }
//
//    db.close();
//    environment.close();
//  }
//
//  public static void runKeyset(boolean doInsert) throws DatabaseException {
//    // doesn't quite work - probably better to use cursors
//    EnvironmentConfig envConfig = new EnvironmentConfig();
//    envConfig.setTransactional(true);
//    envConfig.setAllowCreate(true);
//    Environment environment = new Environment(envDir, envConfig);
//
//    Transaction txn = environment.beginTransaction(null, null);
//    DatabaseConfig dbConfig = new DatabaseConfig();
//    dbConfig.setTransactional(true);
//    dbConfig.setAllowCreate(true);
//    dbConfig.setSortedDuplicates(true);
//    Database db = environment.openDatabase(txn,"simpleDb", dbConfig);
//    ClassCatalog catalog = new StoredClassCatalog(db);
//    txn.commit();
//
//    DatabaseEntry keyEntry = new DatabaseEntry();
//    DatabaseEntry dataEntry = new DatabaseEntry();
//
//    SerialBinding<String> dataBinding =
//      new SerialBinding<String>(catalog, String.class);
//
//
//    if (doInsert) {
//      txn = environment.beginTransaction(null, null);
//
//      MyData.tupleBinding.objectToEntry(new MyData(20, "testLabel"), keyEntry);
//      PackedLongBinding longListBinding = new PackedLongBinding();
//      StoredKeySet<Long> storedKeySet = new StoredKeySet<>(db, longListBinding, true);
//      storedKeySet.add(5l);
//      storedKeySet.add(6l);
//      // TODO relate dataEntry to storedKeySet or longListBinding
////      longListBinding.objectToEntry(10l, dataEntry);
//
//      OperationStatus status = db.put(txn, keyEntry, dataEntry);
//
//      if (status != OperationStatus.SUCCESS) {
//        throw new RuntimeException("Data insertion got status " + status);
//      }
//      txn.commit();
//    } else {
//      MyData.tupleBinding.objectToEntry(new MyData(20, "testLabel"), keyEntry);
//      System.out.println(db.get(null, keyEntry, dataEntry, null));
//      System.out.println(dataEntry);
////      System.out.println(IntegerBinding.entryToInt(dataEntry));
//    }
//
//    db.close();
//    environment.close();
//  }
//
//  public static void runTuple(boolean doInsert) throws DatabaseException {
//    EnvironmentConfig envConfig = new EnvironmentConfig();
//    envConfig.setTransactional(true);
//    envConfig.setAllowCreate(true);
//    Environment environment = new Environment(envDir, envConfig);
//
//    Transaction txn = environment.beginTransaction(null, null);
//    DatabaseConfig dbConfig = new DatabaseConfig();
//    dbConfig.setTransactional(true);
//    dbConfig.setAllowCreate(true);
//    dbConfig.setSortedDuplicates(true);
//    Database db = environment.openDatabase(txn,"simpleDb", dbConfig);
//    txn.commit();
//
//    DatabaseEntry keyEntry = new DatabaseEntry();
//    DatabaseEntry dataEntry = new DatabaseEntry();
//
//    if (doInsert) {
//      for (int i = offset; i < numRecords + offset; i++) {
//        txn = environment.beginTransaction(null, null);
//
//        MyData.tupleBinding.objectToEntry(new MyData(i, "testLabel"), keyEntry);
//        IntegerBinding.intToEntry(i+100, dataEntry);
//
//        OperationStatus status = db.put(txn, keyEntry, dataEntry);
//
//        if (status != OperationStatus.SUCCESS) {
//          throw new RuntimeException("Data insertion got status " + status);
//        }
//        txn.commit();
//      }
//    } else {
//      MyData.tupleBinding.objectToEntry(new MyData(2, "testLabel"), keyEntry);
//      System.out.println(db.get(null, keyEntry, dataEntry, null));
//      System.out.println(IntegerBinding.entryToInt(dataEntry));
//    }
//
//    db.close();
//    environment.close();
//  }
//
//  public static void runSimple(boolean doInsert) throws DatabaseException {
//    EnvironmentConfig envConfig = new EnvironmentConfig();
//    envConfig.setTransactional(true);
//    envConfig.setAllowCreate(true);
//    Environment environment = new Environment(envDir, envConfig);
//
//    // TODO: autocommit? simply passing a null txn handle to openDatabase()
//    Transaction txn = environment.beginTransaction(null, null);
//    DatabaseConfig dbConfig = new DatabaseConfig();
//    dbConfig.setTransactional(true);
//    dbConfig.setAllowCreate(true);
//    dbConfig.setSortedDuplicates(true);
//    Database db = environment.openDatabase(txn,"simpleDb", dbConfig);
//    txn.commit();
//
//    DatabaseEntry keyEntry = new DatabaseEntry();
//    DatabaseEntry dataEntry = new DatabaseEntry();
//
//    if (doInsert) {
//      for (int i = offset; i < numRecords + offset; i++) {
//        txn = environment.beginTransaction(null, null);
//        IntegerBinding.intToEntry(i, keyEntry);
//        IntegerBinding.intToEntry(i+10, dataEntry);
//        OperationStatus status = db.put(txn, keyEntry, dataEntry);
//
//        if (status != OperationStatus.SUCCESS) {
//          throw new RuntimeException("Data insertion got status " + status);
//        }
//        txn.commit();
//      }
//    } else {
//      IntegerBinding.intToEntry(1, keyEntry);
//      System.out.println(dataEntry); // null
//      System.out.println(db.get(null, keyEntry, dataEntry, null));
//      System.out.println(IntegerBinding.entryToInt(dataEntry));
//
//      // fetch all
////      Cursor cursor = db.openCursor(null, null);
////      while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
////        System.out.println("key=" + IntegerBinding.entryToInt(keyEntry) + " data=" + IntegerBinding.entryToInt(dataEntry));
////      }
////      cursor.close();
//    }
//
//    db.close();
//    environment.close();
//  }
//}
