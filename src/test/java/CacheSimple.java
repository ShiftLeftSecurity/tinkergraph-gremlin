import org.apache.tinkerpop.gremlin.structure.*;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;

import java.util.Iterator;


public class CacheSimple {

  public static void main(String[] args) {

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("preConfigured", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Vertex.class, ResourcePoolsBuilder.heap(10)))
      .withCache("preConfigured2", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Vertex.class, ResourcePoolsBuilder.heap(10)))
      .build();
    cacheManager.init();

    Cache<Long, Vertex> myCache = cacheManager.getCache("preConfigured2", Long.class, Vertex.class);

//    Cache<Long, Vertex> myCache = cacheManager.createCache("myCache",
//      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Vertex.class, ResourcePoolsBuilder.heap(10)));

    myCache.put(1L, new Vertex() {
      @Override
      public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        return null;
      }

      @Override
      public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        return null;
      }

      @Override
      public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return null;
      }

      @Override
      public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return null;
      }

      @Override
      public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        return null;
      }

      @Override
      public Object id() {
        return 99l;
      }

      @Override
      public String label() {
        return null;
      }

      @Override
      public Graph graph() {
        return null;
      }

      @Override
      public void remove() {

      }
    });
    Vertex value = myCache.get(1L);

    System.out.println(value.id());

    cacheManager.removeCache("preConfigured");

    cacheManager.close();

  }

}
