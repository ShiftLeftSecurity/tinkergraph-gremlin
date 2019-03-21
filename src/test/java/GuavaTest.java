import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class GuavaTest {
//  static final int count = 5000000;
  static final int count = 1400000;

  //
  public static void main(String[] args) throws Exception {
    Cache<Long, Instance3> cache = CacheBuilder.newBuilder()
//      .softValues()
      .maximumSize(1500000)
      .build();

    long start = System.currentTimeMillis();
    // run times for 1400000 entries (no clearing necessary):
    // soft values: 4500ms; max entries: 3500ms

    // run times for 5M entries, incl. clearing
    // soft values: 18s; max entries 1.5M: 22s -> note soft values cleared far too many refs
    for (int i = 0; i<count; i++) {
      cache.put(new Long(i), new Instance3());

      if (i % 100000 == 0) {
//        Thread.sleep(100); // in lieu of other application usage
        System.out.println(i + " free=" + Runtime.getRuntime().freeMemory() / 1024 / 1024 + "M");
      }
    }
    System.out.println("time taken: " + (System.currentTimeMillis() - start));
  }



}

class Instance3 {
  static int finalizedCount = 0;
  String[] occupySomeHeap = new String[50];

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    finalizedCount++;
    if (finalizedCount % 100000 == 0) {
      System.out.println(finalizedCount + " instances finalized");
    }
  }
}