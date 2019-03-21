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

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.TLongSet;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.util.*;

public class VertexSerializer extends Serializer<Vertex> {

  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel;

  public VertexSerializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel) {
    this.graph = graph;
    this.vertexFactoryByLabel = vertexFactoryByLabel;
  }

  @Override
  public byte[] serialize(Vertex vertex) throws IOException {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    ((SpecializedTinkerVertex) vertex).acquireModificationLock();
    packer.packLong((Long) vertex.id());
    packer.packString(vertex.label());
    packProperties(packer, vertex.properties());
    packEdgeIds(packer, vertex);
    ((SpecializedTinkerVertex) vertex).releaseModificationLock();

    return packer.toByteArray();
  }

  /** format: two `Map<Label, Array<EdgeId>>`, i.e. one Map for `IN` and one for `OUT` edges */
  private void packEdgeIds(MessageBufferPacker packer, Vertex vertex) throws IOException {
    for (Direction direction : new Direction[]{Direction.IN, Direction.OUT}) {
      final Map<String, TLongSet> edgeIdsByLabel;
      if (vertex instanceof SpecializedTinkerVertex) {
        edgeIdsByLabel = ((SpecializedTinkerVertex) vertex).edgeIdsByLabel(direction);
      } else {
        throw new NotImplementedException("");
      }

      // a simple group by would be nice, but java collections are still very basic apparently
      packer.packMapHeader(edgeIdsByLabel.size());
      Set<String> labels = edgeIdsByLabel.keySet();
      for (String label : labels) {
        final TLongSet edgeIds = edgeIdsByLabel.get(label);
        final TLongIterator iter = edgeIds.iterator();
        packer.packString(label);
        packer.packArrayHeader(edgeIds.size());
        while (iter.hasNext()) {
          packer.packLong(iter.next());
        }
      }
    }
  }

  @Override
  public SpecializedTinkerVertex deserialize(byte[] bytes) throws IOException {
    if (null == bytes)
      return null;

    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
    Long id = unpacker.unpackLong();
    String label = unpacker.unpackString();
    Object[] keyValues = unpackProperties(unpacker.unpackValue().asMapValue().map());

    SpecializedElementFactory.ForVertex vertexFactory = vertexFactoryByLabel.get(label);
    if (vertexFactory == null) {
      throw new AssertionError("vertexFactory not found for id=" + id + ", label=" + label);
    }
    SpecializedTinkerVertex vertex = vertexFactory.createVertex(id, graph);
    ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);

    Map<String, long[]> inEdgeIdsByLabel = unpackEdges(unpacker);
    Map<String, long[]> outEdgeIdsByLabel = unpackEdges(unpacker);

    inEdgeIdsByLabel.entrySet().stream().forEach(entry -> {
      String edgeLabel = entry.getKey();
      for (long edgeId : entry.getValue()) {
        vertex.addSpecializedInEdge(edgeLabel, edgeId);
      }
    });

    outEdgeIdsByLabel.entrySet().stream().forEach(entry -> {
      String edgeLabel = entry.getKey();
      for (long edgeId : entry.getValue()) {
        vertex.addSpecializedOutEdge(edgeLabel, edgeId);
      }
    });

    vertex.setModifiedSinceLastSerialization(false);
    return vertex;
  }

  /** format: `Map<Label, Array<EdgeId>>` */
  private Map<String, long[]> unpackEdges(MessageUnpacker unpacker) throws IOException {
    int labelCount = unpacker.unpackMapHeader();
    Map<String, long[]> edgeIdsByLabel = new HashMap<>(labelCount);
    for (int i = 0; i < labelCount; i++) {
      String label = unpacker.unpackString();
      int edgeIdsCount = unpacker.unpackArrayHeader();
      long[] edgeIds = new long[edgeIdsCount];
      for (int j = 0; j < edgeIdsCount; j++) {
        edgeIds[j] = unpacker.unpackLong();
      }
      edgeIdsByLabel.put(label, edgeIds);
    }
    return edgeIdsByLabel;
  }

}
