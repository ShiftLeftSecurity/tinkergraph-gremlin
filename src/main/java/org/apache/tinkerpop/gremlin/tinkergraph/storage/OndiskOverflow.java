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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class OndiskOverflow {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final VertexSerializer vertexSerializer;
  protected final EdgeSerializer edgeSerializer;
  private final MVStore mvstore;
  protected final MVMap<Long, byte[]> vertexMVMap;
  protected final MVMap<Long, byte[]> edgeMVMap;
  private boolean closed;

  public OndiskOverflow(final String ondiskOverflowRootDir,
                        final VertexSerializer vertexSerializer, final EdgeSerializer edgeSerializer) {
    this.vertexSerializer = vertexSerializer;
    this.edgeSerializer = edgeSerializer;

    final File mvstoreFile;
    try {
      File cacheParentDir = ondiskOverflowRootDir != null ? new File(ondiskOverflowRootDir) : null;
      mvstoreFile = File.createTempFile("mvstore", ".bin", cacheParentDir);
      mvstoreFile.deleteOnExit();
      logger.debug("on-disk cache overflow files: " + mvstoreFile);
    } catch (IOException e) {
      throw new RuntimeException("cannot create tmp file for mvstore", e);
    }
    mvstore = new MVStore.Builder().fileName(mvstoreFile.getAbsolutePath()).open();
    vertexMVMap = mvstore.openMap("vertices");
    edgeMVMap = mvstore.openMap("edges");
  }

//  private

  public void persist(final TinkerElement element) throws IOException {
    if (!closed) {
      final Long id = (Long) element.id();
      if (element instanceof Vertex) {
        vertexMVMap.put(id, vertexSerializer.serialize((Vertex) element));
      } else if (element instanceof Edge) {
        edgeMVMap.put(id, edgeSerializer.serialize((Edge) element));
      } else {
        new RuntimeException("unable to serialize " + element + " of type " + element.getClass());
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
    mvstore.close();
  }

  public void removeVertex(final Long id) {
    vertexMVMap.remove(id);
  }

  public void removeEdge(final Long id) {
    edgeMVMap.remove(id);
  }

}
