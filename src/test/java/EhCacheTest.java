import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;

import java.util.Random;

public class EhCacheTest {

  static final int count = 5000000;

  /**
   * to get usable output, run with e.g. `-Xms1g -Xmx1g -XX:SoftRefLRUPolicyMSPerMB=0`
   */
  public static void main(String[] args) throws Exception {
    final long start = getUsedMemory();

//    ResourcePools resourcePools = ResourcePoolsBuilder.newResourcePoolsBuilder().//.heap(cacheMaxHeapMegabytes, MemoryUnit.MB).build();
//    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
//      .withCache("test", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, SpecializedTinkerVertex.class, resourcePools))
//      .build();
//    cacheManager.init();
//    vertexCache = cacheManager.getCache(verticesCacheName, Long.class, SpecializedTinkerVertex.class);
//    Object ids = new long[10000000]; //76MB for 100M
//    Long[] ids = new Long[10000000]; //36M unfilled; 266M filled
//    Random random = new Random();
//    for (int i = 0; i < ids.length; i++) {
//      ids[i] = random.nextLong();
//    }

//    final long size = (getUsedMemory() - start ) / 1024 / 1024;
//    System.out.println("used heap: " + size + 'M' );

//    Cache<Long, Instance2> cache;

//    ArrayList<Instance2> instances = new ArrayList<>();
//    for (int i = 0; i<count; i++) {
//      instances.add(new Instance2());
//
//      if (i % 100000 == 0) {
//        Thread.sleep(100); // in lieu of other application usage
//        System.out.println(i + " free=" + Runtime.getRuntime().freeMemory() / 1024 / 1024 + "M");
//      }
//    }
  }

  private static long getUsedMemory() {
    System.gc();
    return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
  }

}

class Instance2 {
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