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
import org.apache.tinkerpop.gremlin.structure.*;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.UserManagedCache;
import org.ehcache.config.Builder;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.*;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.events.CacheEventDispatcherFactory;
import org.ehcache.core.internal.events.EventListenerWrapper;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.impl.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.impl.internal.events.CacheEventDispatcherFactoryImpl;
import org.ehcache.impl.internal.sizeof.listeners.EhcacheVisitorListener;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;


public class CacheSimple {

  public static void main(String[] args) throws InterruptedException {
    int vertexCount = 5;

    Vertex[] vertices = new Vertex[vertexCount];
    for (int i = 0; i<vertexCount; i++) {
      final int j = i;
      vertices[i] = new Vertex() {

        final Long id = new Long(j);

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
          return id;
        }

        @Override
        public String label() {
          return "testvertex";
        }

        @Override
        public Graph graph() {
          return null;
        }

        @Override
        public void remove() {        }
      };
    }

    Serializer<Vertex> vertexSerializer = new Serializer<Vertex>() {
      @Override
      public ByteBuffer serialize(Vertex vertex) throws SerializerException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
          packer.packLong((Long) vertex.id());
          packer.packString(vertex.label());
          return ByteBuffer.wrap(packer.toByteArray());
        } catch (IOException e) {
          e.printStackTrace();
          throw new SerializerException(e);
        }
      }

      @Override
      public Vertex read(ByteBuffer byteBuffer) throws SerializerException {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(byteBuffer.array());
        try {
          Long id = unpacker.unpackLong();
          System.out.println("CacheSimple.read " + id);
          return vertices[id.intValue()];
        } catch (IOException e) {
          e.printStackTrace();
          throw new SerializerException(e);
        }
      }

      @Override
      public boolean equals(Vertex vertex, ByteBuffer byteBuffer) throws SerializerException {
        System.out.println("CacheSimple.equals");
        return false;
      }
    };

    CacheEventListener<Long, Vertex> listener = new CacheEventListener<Long, Vertex>() {
      @Override
      public void onEvent(CacheEvent<? extends Long, ? extends Vertex> event) {
        if (event.getType().equals(EventType.REMOVED)) {
          System.out.println("TODO datastore.remove " + event.getKey());
        } else if (event.getType().equals(EventType.EVICTED)) {
          System.out.println("TODO datastore.put key=" + event.getKey() + ", value=" + event.getOldValue() + ", id=" + event.getOldValue().id());
        }
      }
    };

    CacheEventListenerConfigurationBuilder cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
      .newEventListenerConfiguration(listener, EventType.EVICTED, EventType.REMOVED).asynchronous();

    final CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("foo",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, Vertex.class, ResourcePoolsBuilder.heap(2))
          .add(cacheEventListenerConfiguration)
      ).build(true);

    final Cache<Long, Vertex> cache = cacheManager.getCache("foo", Long.class, Vertex.class);

    for (int i = 0; i<vertexCount; i++) {
      cache.put((long)i, vertices[i]);
    }
    Thread.sleep(1000);
    cacheManager.close();
  }

}
