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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRef;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VertexDeserializer extends Deserializer<Vertex> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel;
  private int deserializedCount = 0;
  private long deserializationTimeSpentMillis = 0;

  public VertexDeserializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForVertex> vertexFactoryByLabel) {
    this.graph = graph;
    this.vertexFactoryByLabel = vertexFactoryByLabel;
  }

  @Override
  public TinkerVertex deserialize(byte[] bytes) throws IOException {
    long start = System.currentTimeMillis();
    if (null == bytes)
      return null;

    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
      Long id = unpacker.unpackLong();
      String label = unpacker.unpackString();
      List keyValues = unpackProperties(unpacker.unpackValue().asMapValue().map());

      SpecializedElementFactory.ForVertex vertexFactory = vertexFactoryByLabel.get(label);
      if (vertexFactory == null) {
        throw new AssertionError("vertexFactory not found for label=" + label);
      }
      SpecializedTinkerVertex vertex = vertexFactory.createVertex(id, graph);
      ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues.toArray());

      Map<String, long[]> inEdgeIdsByLabel = unpackEdges(unpacker);
      Map<String, long[]> outEdgeIdsByLabel = unpackEdges(unpacker);

      inEdgeIdsByLabel.entrySet().stream().forEach(entry -> {
        for (long edgeId : entry.getValue()) {
          vertex.storeInEdge(graph.edge(edgeId));
        }
      });

      outEdgeIdsByLabel.entrySet().stream().forEach(entry -> {
        for (long edgeId : entry.getValue()) {
          vertex.storeOutEdge(graph.edge(edgeId));
        }
      });

      vertex.setModifiedSinceLastSerialization(false);

      deserializedCount++;
      deserializationTimeSpentMillis += System.currentTimeMillis() - start;
      if (deserializedCount % 100000 == 0) {
        float avgDeserializationTime = deserializationTimeSpentMillis / (float) deserializedCount;
        logger.debug("stats: deserialized " + deserializedCount + " vertices in total (avg time: " + avgDeserializationTime + "ms)");
      }
      return vertex;
    }
  }

  @Override
  public VertexRef<TinkerVertex> deserializeRef(byte[] bytes) throws IOException {
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
      Long id = unpacker.unpackLong();
      String label = unpacker.unpackString();

      SpecializedElementFactory.ForVertex vertexFactory = vertexFactoryByLabel.get(label);
      if (vertexFactory == null) {
        throw new AssertionError("vertexFactory not found for label=" + label);
      }

      return vertexFactory.createVertexRef(id, graph);
    }
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
