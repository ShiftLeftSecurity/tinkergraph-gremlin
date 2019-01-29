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
import org.ehcache.config.Builder;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;


public class CacheSimple {

  public static void main(String[] args) {

    Vertex v1 = new Vertex() {
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
        return 1l;
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
    };

    Vertex v2 = new Vertex() {
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
        return 2l;
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
    };


    Serializer<Vertex> vertexSerializer = new Serializer<Vertex>() {
      @Override
      public ByteBuffer serialize(Vertex vertex) throws SerializerException {
        Long id = (Long) vertex.id();
        System.out.println("CacheSimple.serialize " + id);
//        throw new RuntimeException("foo");
        return ByteBuffer.wrap(new byte[]{id.byteValue()});
      }

      @Override
      public Vertex read(ByteBuffer byteBuffer) throws SerializerException {
        System.out.println("CacheSimple.read " + byteBuffer.get(0));
        return v1;
      }

      @Override
      public boolean equals(Vertex vertex, ByteBuffer byteBuffer) throws SerializerException {
        System.out.println("CacheSimple.equals");
        return false;
      }
    };
    boolean persistent = false;
    Builder<? extends ResourcePools> resourcePools = ResourcePoolsBuilder
      .heap(1)
      .disk(1, MemoryUnit.GB, persistent);
    Copier<Vertex> vertexCopier = new Copier<Vertex>() {
      @Override
      public Vertex copyForRead(Vertex vertex) {
        System.out.println("CacheSimple.copyForRead");
        return vertex;
      }

      @Override
      public Vertex copyForWrite(Vertex vertex) {
        System.out.println("CacheSimple.copyForWrite");
        return vertex;
      }
    };
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence(new File("ehcache.bin")))
      .withCache(
        "preConfigured",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(
          Long.class, Vertex.class, resourcePools)
        .withValueSerializer(vertexSerializer)
        .withValueCopier(vertexCopier))
      .build();
    cacheManager.init();

    Cache<Long, Vertex> myCache = cacheManager.getCache("preConfigured", Long.class, Vertex.class);

    myCache.put(1L, v1);
    myCache.put(2L, v2);
    Vertex value = myCache.get(1L);

    cacheManager.close();

  }

}
