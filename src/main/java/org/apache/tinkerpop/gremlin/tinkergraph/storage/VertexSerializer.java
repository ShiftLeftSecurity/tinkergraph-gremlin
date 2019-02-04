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

import org.apache.commons.collections.IteratorUtils;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VertexSerializer extends Serializer<Vertex> {

  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel;

  public VertexSerializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel) {
    this.graph = graph;
    this.vertexFactoryByLabel = vertexFactoryByLabel;
  }

  static Set<Long> serializedIds = new HashSet();

  @Override
  public byte[] serialize(Vertex vertex) throws IOException {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    packer.packLong((Long) vertex.id());
    packer.packString(vertex.label());
    packProperties(packer, vertex.properties());
    packEdgeIds(packer, vertex);

    return packer.toByteArray();
  }

  /** format: two `Map<Label, Array<EdgeId>>`, i.e. one Map for `IN` and one for `OUT` edges */
  private void packEdgeIds(MessageBufferPacker packer, Vertex vertex) throws IOException {
    for (Direction direction : new Direction[]{Direction.IN, Direction.OUT}) {
      final Map<String, Set<Long>> edgeIdsByLabel;
      if (vertex instanceof SpecializedTinkerVertex) {
        edgeIdsByLabel = ((SpecializedTinkerVertex) vertex).edgeIdsByLabel(direction);
      } else {
        edgeIdsByLabel = new HashMap<>();
        List<Edge> edges = IteratorUtils.toList(vertex.edges(direction));
        // a simple group by would be nice, but java collections are still very basic apparently
        Set<String> labels = edges.stream().map(e -> e.label()).collect(Collectors.toSet());
        for (String label : labels) {
          Set<Long> edgeIds = edges.stream().filter(e -> e.label().equals(label)).map(e -> (Long) e.id()).collect(Collectors.toSet());
          edgeIdsByLabel.put(label, edgeIds);
        }
      }

      // a simple group by would be nice, but java collections are still very basic apparently
      packer.packMapHeader(edgeIdsByLabel.size());
      Set<String> labels = edgeIdsByLabel.keySet();
      for (String label : labels) {
        packer.packString(label);
        Set<Long> edgeIds = edgeIdsByLabel.get(label);
        packer.packArrayHeader(edgeIds.size());
        for (Long edgeId : edgeIds) {
          packer.packLong(edgeId);
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

    SpecializedTinkerVertex vertex = vertexFactoryByLabel.get(label).createVertex(id, graph);
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
