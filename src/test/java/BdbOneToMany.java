//import java.io.File;
//import java.io.Serializable;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.Set;
//
//import com.sleepycat.bind.EntryBinding;
//import com.sleepycat.bind.serial.SerialBinding;
//import com.sleepycat.bind.serial.StoredClassCatalog;
//import com.sleepycat.bind.tuple.StringBinding;
//import com.sleepycat.je.Database;
//import com.sleepycat.je.DatabaseConfig;
//import com.sleepycat.je.DatabaseEntry;
//import com.sleepycat.je.DatabaseException;
//import com.sleepycat.je.Environment;
//import com.sleepycat.je.EnvironmentConfig;
//import com.sleepycat.je.OperationStatus;
//import com.sleepycat.je.SecondaryConfig;
//import com.sleepycat.je.SecondaryCursor;
//import com.sleepycat.je.SecondaryDatabase;
//import com.sleepycat.je.SecondaryMultiKeyCreator;
//import com.sleepycat.je.Transaction;
//
//
//public class BdbOneToMany {
//
//  private final Environment env;
//  private Database catalogDb;
//  private Database personDb;
//  private SecondaryDatabase personByEmail;
//  private EntryBinding<String> keyBinding;
//  private EntryBinding<Person> personBinding;
//
//  public static void main(String[] args) {
//    String homeDir = "bdbenv";
//    for (File f : new File(homeDir).listFiles()) {
//      f.delete();
//    }
//
//    try {
//      BdbOneToMany example = new BdbOneToMany(homeDir);
//      example.exec();
//      example.close();
//    } catch (DatabaseException e) {
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   * Opens the environment and all databases.
//   */
//  private BdbOneToMany(String homeDir) throws DatabaseException {
//
//    /* Open the environment. */
//    EnvironmentConfig envConfig = new EnvironmentConfig();
//    envConfig.setAllowCreate(true);
//    envConfig.setTransactional(true);
//    env = new Environment(new File(homeDir), envConfig);
//
//    /* Open/create all databases in a transaction. */
//    Transaction txn = env.beginTransaction(null, null);
//    try {
//      /* A standard (no duplicates) database config. */
//      DatabaseConfig dbConfig = new DatabaseConfig();
//      dbConfig.setAllowCreate(true);
//      dbConfig.setTransactional(true);
//
//      /* The catalog is used for the serial binding. */
//      catalogDb = env.openDatabase(txn, "catalog", dbConfig);
//      StoredClassCatalog catalog = new StoredClassCatalog(catalogDb);
//      personBinding = new SerialBinding(catalog, null);
//      keyBinding = new StringBinding();
//
//      /* Open the person primary DBs. */
//      personDb = env.openDatabase(txn, "person", dbConfig);
//
//      /*
//       * A standard secondary config; duplicates, key creators and key
//       * nullifiers are specified below.
//       */
//      SecondaryConfig secConfig = new SecondaryConfig();
//      secConfig.setAllowCreate(true);
//      secConfig.setTransactional(true);
//
//      /*
//       * Open the secondary database for personByEmail.  This is a
//       * one-to-many index because duplicates are not configured.
//       */
//      secConfig.setSortedDuplicates(false);
//      secConfig.setMultiKeyCreator(new EmailKeyCreator());
//      personByEmail = env.openSecondaryDatabase(txn, "personByEmail", personDb, secConfig);
//
//      txn.commit();
//    } catch (DatabaseException e) {
//      txn.abort();
//      throw e;
//    } catch (RuntimeException e) {
//      txn.abort();
//      throw e;
//    }
//  }
//
//  /**
//   * Closes all databases and the environment.
//   */
//  private void close() throws DatabaseException {
//
//    if (personByEmail != null) {
//      personByEmail.close();
//    }
//    if (catalogDb != null) {
//      catalogDb.close();
//    }
//    if (personDb != null) {
//      personDb.close();
//    }
//    if (env != null) {
//      env.close();
//    }
//  }
//
//  private void exec()
//    throws DatabaseException {
//
//    Person kathy = new Person();
//    kathy.name = "Kathy";
//    kathy.emailAddresses.add("kathy@kathy.com");
//    kathy.emailAddresses.add("kathy@yahoo.com");
//    putPerson(kathy);
//    printPerson("Kathy");
//
//    System.out.println
//      ("\nPrint by email address index.");
//    printByIndex(personByEmail);
//  }
//
//  /**
//   * Gets a person by name and prints it.
//   */
//  private void printPerson(String name) throws DatabaseException {
//
//    DatabaseEntry key = new DatabaseEntry();
//    keyBinding.objectToEntry(name, key);
//
//    DatabaseEntry data = new DatabaseEntry();
//
//    OperationStatus status = personDb.get(null, key, data, null);
//    if (status == OperationStatus.SUCCESS) {
//      Person person = personBinding.entryToObject(data);
//      person.name = keyBinding.entryToObject(key);
//      System.out.println(person);
//    } else {
//      System.out.println("Person not found: " + name);
//    }
//  }
//
//  /**
//   * Prints all person records by a given secondary index.
//   */
//  private void printByIndex(SecondaryDatabase secDb)
//    throws DatabaseException {
//
//    DatabaseEntry secKey = new DatabaseEntry();
//    DatabaseEntry priKey = new DatabaseEntry();
//    DatabaseEntry priData = new DatabaseEntry();
//
//    SecondaryCursor cursor = secDb.openSecondaryCursor(null, null);
//    try {
//      while (cursor.getNext(secKey, priKey, priData, null) == OperationStatus.SUCCESS) {
//        Person person = personBinding.entryToObject(priData);
//        person.name = keyBinding.entryToObject(priKey);
//        System.out.println("Index key [" +
//          keyBinding.entryToObject(secKey) +
//          "] maps to primary key [" +
//          person.name + ']');
//      }
//    } finally {
//      cursor.close();
//    }
//  }
//
//  /**
//   * Inserts or updates a person.  Uses auto-commit.
//   */
//  private void putPerson(Person person)
//    throws DatabaseException {
//
//    DatabaseEntry key = new DatabaseEntry();
//    keyBinding.objectToEntry(person.name, key);
//
//    DatabaseEntry data = new DatabaseEntry();
//    personBinding.objectToEntry(person, data);
//
//    personDb.put(null, key, data);
//  }
//
//  @SuppressWarnings("serial")
//  private static class Person implements Serializable {
//
//    /** The primary key. */
//    private transient String name;
//
//    /** A one-to-many set of keys. */
//    private final Set<String> emailAddresses = new HashSet<String>();
//
//    @Override
//    public String toString() {
//      return "Person {" +
//        "\n  Name: " + name +
//        "\n  EmailAddresses: " + emailAddresses +
//        "\n}";
//    }
//  }
//
//  /**
//   * Returns the set of email addresses for a person.  This is an example
//   * of a multi-key creator for a to-many index.
//   */
//  private class EmailKeyCreator implements SecondaryMultiKeyCreator {
//
//    public void createSecondaryKeys(SecondaryDatabase secondary,
//                                    DatabaseEntry primaryKey,
//                                    DatabaseEntry primaryData,
//                                    Set<DatabaseEntry> results) {
//      Person person = personBinding.entryToObject(primaryData);
//      copyKeysToEntries(person.emailAddresses, results);
//    }
//  }
//
//  /**
//   * A utility method to copy a set of keys (Strings) into a set of
//   * DatabaseEntry objects.
//   */
//  private void copyKeysToEntries(Set<String> keys,
//                                 Set<DatabaseEntry> entries) {
//
//    for (Iterator<String> i = keys.iterator(); i.hasNext();) {
//      DatabaseEntry entry = new DatabaseEntry();
//      keyBinding.objectToEntry(i.next(), entry);
//      entries.add(entry);
//    }
//  }
//
//}
