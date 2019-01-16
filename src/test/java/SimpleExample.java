import java.io.File;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

class SimpleExample {
  private static int numRecords = 3;   // num records to insert or retrieve
  private static int offset = 0;       // where we want to start inserting
  private static File envDir = new File("bdbenv");

  public static void main(String args[]) {
//    run(true);
    run(false);
  }

  public static void run(boolean doInsert) throws DatabaseException {
    EnvironmentConfig envConfig = new EnvironmentConfig();
    envConfig.setTransactional(true);
    envConfig.setAllowCreate(true);
    Environment environment = new Environment(envDir, envConfig);

    // TODO: autocommit? simply passing a null txn handle to openDatabase()
    Transaction txn = environment.beginTransaction(null, null);
    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setTransactional(true);
    dbConfig.setAllowCreate(true);
    dbConfig.setSortedDuplicates(true);
    Database db = environment.openDatabase(txn,"simpleDb", dbConfig);
    txn.commit();

    DatabaseEntry keyEntry = new DatabaseEntry();
    DatabaseEntry dataEntry = new DatabaseEntry();

    if (doInsert) {
      for (int i = offset; i < numRecords + offset; i++) {
        txn = environment.beginTransaction(null, null);
        IntegerBinding.intToEntry(i, keyEntry);
        IntegerBinding.intToEntry(i+10, dataEntry);
        OperationStatus status = db.put(txn, keyEntry, dataEntry);

        if (status != OperationStatus.SUCCESS) {
          throw new RuntimeException("Data insertion got status " + status);
        }
        txn.commit();
      }
    } else {
      IntegerBinding.intToEntry(1, keyEntry);
      System.out.println(dataEntry); // null
      System.out.println(db.get(null, keyEntry, dataEntry, null));
      System.out.println(IntegerBinding.entryToInt(dataEntry));

//      Cursor cursor = db.openCursor(null, null);
//      while (cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
//        System.out.println("key=" + IntegerBinding.entryToInt(keyEntry) + " data=" + IntegerBinding.entryToInt(dataEntry));
//      }
//      cursor.close();
    }

    db.close();
    environment.close();
  }
}
