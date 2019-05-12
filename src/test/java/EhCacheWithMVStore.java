/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
// import org.ehcache.Cache;
// import org.ehcache.CacheManager;
// import org.ehcache.config.builders.CacheConfigurationBuilder;
// import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
// import org.ehcache.config.builders.CacheManagerBuilder;
// import org.ehcache.config.builders.ResourcePoolsBuilder;
// import org.ehcache.event.CacheEventListener;
// import org.ehcache.event.EventType;
// import org.ehcache.config.units.EntryUnit;
// import org.h2.mvstore.MVMap;
// import org.h2.mvstore.MVStore;
// import org.h2.mvstore.OffHeapStore;

// import java.util.Random;

// public class EhCacheTest {

//   public static int storedInSecondaryCacheCount = 0;

//   public static void main(String[] args) throws Exception {
// //    File ondiskCacheFile = new File("ehcache-" + System.currentTimeMillis());
//     Random random = new Random();

//     OffHeapStore offHeap = new OffHeapStore();
//     MVStore vertexMVStore = new MVStore.Builder().
//         fileStore(offHeap).open();
//     MVMap<Long, byte[]> vertexMVMap = vertexMVStore.openMap("vertices");

//     CacheEventListener<Long, Node> cacheEventListener = event -> {
//       if (event.getType().equals(EventType.REMOVED)) {
//         vertexMVMap.remove(event.getKey());
//       } else if (event.getType().equals(EventType.EVICTED)) {
//         storedInSecondaryCacheCount++;
//         // evicted from on-heap cache -> serialize and store in off-heap datastore (which overflows to disk)
//         final Node node = event.getOldValue();
//         try {
//           vertexMVMap.put(node.getId(), node.getData());
//         } catch (Throwable t) {
//           t.printStackTrace();
//         }
//       }
//     };
//     CacheEventListenerConfigurationBuilder cacheEventListenerConfig = CacheEventListenerConfigurationBuilder
//         .newEventListenerConfiguration(cacheEventListener, EventType.EVICTED, EventType.REMOVED).asynchronous().unordered();

//     CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
//         .withCache("cache",
//             CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Node.class,
//                 ResourcePoolsBuilder.newResourcePoolsBuilder().heap(1000, EntryUnit.ENTRIES))
//                 .add(cacheEventListenerConfig))
//         .build();
//     cacheManager.init();

// //    long offheapBytes = VM.maxDirectMemory() - 1024*1024;
// //    CacheConfigurationBuilder<Long, Node> cacheConfigBuilder = CacheConfigurationBuilder
// //        .newCacheConfigurationBuilder(Long.class, Node.class,
// //            ResourcePoolsBuilder.newResourcePoolsBuilder()
// //                .heap(10000, EntryUnit.ENTRIES)
// //                .offheap(offheapBytes, MemoryUnit.B)
// //                .disk(99, MemoryUnit.PB, true)) //unbounded
// ////        .add(cacheEventListenerConfig)
// //        .withValueSerializer(new NodeSerializer());
// //    PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
// //        .with(CacheManagerBuilder.persistence(ondiskCacheFile))
// //        .withCache("cache", cacheConfigBuilder)
// //        .build(true);

//     Cache<Long, Node> cache = cacheManager.getCache("cache", Long.class, Node.class);
//     for (long id = 0; id < 100000; id++) {
//       byte[] data = new byte[1024 * 2];
//       random.nextBytes(data);
//       cache.put(id, new Node(id, data));
//       long count = id + 1;
//       if (count % 100000 == 0) {
//         System.out.println("created " + count + " nodes");
//         Thread.sleep(100); // allow diskwriter to catch up, leading to another OOM
//       }
//     }

//     Thread.sleep(1000);
//     System.out.println("storedInSecondaryCacheCount=" + storedInSecondaryCacheCount);
//     System.out.println("vertexMap size=" + vertexMVMap.size());
//     assert(storedInSecondaryCacheCount == vertexMVMap.size());

//     cacheManager.close();
//   }
// }

// class Node {
//   private final long id;
//   private final byte[] data;

//   Node(long id, byte[] data) {
//     this.id = id;
//     this.data = data;
//   }

//   public byte[] getData() {
//     return data;
//   }

//   public long getId() {
//     return id;
//   }
// }

// //class NodeSerializer implements Serializer<Node> {
// //
// //  static long serializedCount = 0;
// //  @Override
// //  public ByteBuffer serialize(Node node) throws SerializerException {
// //    serializedCount++;
// //    if (serializedCount % 10000 == 0) {
// ////      System.out.println("serialized " + serializedCount);
// //    }
// //    return ByteBuffer.wrap(node.getData());
// //  }
// //
// //  @Override
// //  public Node read(ByteBuffer binary) throws ClassNotFoundException, SerializerException {
// //    throw new RuntimeException("not implemented");
// //  }
// //
// //  @Override
// //  public boolean equals(Node object, ByteBuffer binary) throws ClassNotFoundException, SerializerException {
// //    throw new RuntimeException("not implemented");
// //  }
// //}
