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
package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import java.io.File;
import java.io.IOException;

public class OndiskOverflow {

  protected final VertexSerializer vertexSerializer;
  protected final EdgeSerializer edgeSerializer;
  private final MVStore vertexMVStore;
  private final MVStore edgeMVStore;
  protected final MVMap<Long, byte[]> vertexMVMap;
  protected final MVMap<Long, byte[]> edgeMVMap;
  protected long totalPersistCount = 0;
  private boolean closed;

  public OndiskOverflow(final String ondiskOverflowRootDir,
                        final VertexSerializer vertexSerializer, final EdgeSerializer edgeSerializer) {
    this.vertexSerializer = vertexSerializer;
    this.edgeSerializer = edgeSerializer;

    final File mvstoreVerticesFile;
    final File mvstoreEdgesFile;
    try {
      File cacheParentDir = ondiskOverflowRootDir != null ? new File(ondiskOverflowRootDir) : null;
      mvstoreVerticesFile = File.createTempFile("vertexMVStore", ".bin", cacheParentDir);
      mvstoreEdgesFile = File.createTempFile("edgeMVStore", ".bin", cacheParentDir);
      mvstoreVerticesFile.deleteOnExit();
      mvstoreEdgesFile.deleteOnExit();
      System.out.println("on-disk cache overflow files: " + mvstoreVerticesFile + ", " + mvstoreVerticesFile);
    } catch (IOException e) {
      throw new RuntimeException("cannot create tmp file for mvstore", e);
    }
    vertexMVStore = new MVStore.Builder().fileName(mvstoreVerticesFile.getAbsolutePath()).open();
    edgeMVStore = new MVStore.Builder().fileName(mvstoreEdgesFile.getAbsolutePath()).open();
    vertexMVMap = vertexMVStore.openMap("vertices");
    edgeMVMap = edgeMVStore.openMap("edges");
  }

  public void persist(final TinkerElement element) {
    if (!closed) {
      final Long id = (Long) element.id();
      try {

        if (element instanceof Vertex) {
          vertexMVMap.put(id, vertexSerializer.serialize((Vertex) element));
        } else if (element instanceof Edge) {
          edgeMVMap.put(id, edgeSerializer.serialize((Edge) element));
        } else {
          System.out.println("unable to serialize " + element + " of type " + element.getClass());
        }

        totalPersistCount++;
        if (totalPersistCount % 50000 == 0) {
          System.out.println("persisted " + totalPersistCount + " finalized elements in total");
        }

      } catch (Throwable t) {
        // TODO use proper logger
        // note: normally we'd throw a RuntimeException, but since this is typically called from `finalized`, it would be swallowed anyway
        new RuntimeException("unable to serialize " + element, t).printStackTrace();
      }
    }
  }

  public <A extends TinkerVertex> A readVertex(final long id) throws IOException {
    return (A) vertexSerializer.deserialize(vertexMVMap.get(id));
  }

  public <A extends TinkerEdge> A readEdge(final long id) throws IOException {
    return (A) edgeSerializer.deserialize(edgeMVMap.get(id));
  }

  public void close() {
    closed = true;
    vertexMVStore.close();
    edgeMVStore.close();
  }

  public void removeVertex(final Long id) {
    vertexMVMap.remove(id);
  }

  public void removeEdge(final Long id) {
    edgeMVMap.remove(id);
  }

}
