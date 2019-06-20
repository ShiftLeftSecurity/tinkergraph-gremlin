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
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EdgeSerializer extends Serializer<Edge> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final TinkerGraph graph;
  protected final Map<String, SpecializedElementFactory.ForEdge> edgeFactoryByLabel;
  private int serializedCount = 0;
  private int deserializedCount = 0;

  public EdgeSerializer(TinkerGraph graph, Map<String, SpecializedElementFactory.ForEdge> edgeFactoryByLabel) {
    this.graph = graph;
    this.edgeFactoryByLabel = edgeFactoryByLabel;
  }

  @Override
  public byte[] serialize(Edge edge) throws IOException {
    try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
      packer.packLong((Long) edge.id());
      packer.packString(edge.label());
      packer.packLong((Long) edge.outVertex().id());
      packer.packLong((Long) edge.inVertex().id());
      packProperties(packer, edge.properties());

      serializedCount++;
      if (serializedCount % 100000 == 0) {
        logger.debug("stats: serialized " + serializedCount + " edges in total");
      }
      return packer.toByteArray();
    }
  }


  @Override
  public TinkerEdge deserialize(byte[] bytes) throws IOException {
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
      Long id = unpacker.unpackLong();
      String label = unpacker.unpackString();
      long outVertexId = unpacker.unpackLong();
      long inVertexId = unpacker.unpackLong();
      VertexRef outVertexRef = (VertexRef) graph.vertex(outVertexId);
      VertexRef inVertexRef = (VertexRef) graph.vertex(inVertexId);
      List keyValues = unpackProperties(unpacker.unpackValue().asMapValue().map());

      // TODO support generic edges too
      SpecializedTinkerEdge edge = edgeFactoryByLabel.get(label).createEdge(id, graph, outVertexRef, inVertexRef);
      ElementHelper.attachProperties(edge, keyValues.toArray());

      edge.setModifiedSinceLastSerialization(false);

      deserializedCount++;
      if (deserializedCount % 100000 == 0) {
        logger.debug("stats: deserialized " + deserializedCount + " edges in total");
      }
      return edge;
    }
  }

  @Override
  public EdgeRef<TinkerEdge> deserializeRef(byte[] bytes) throws IOException {
    try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
      Long id = unpacker.unpackLong();
      String label = unpacker.unpackString();
      long outVertexId = unpacker.unpackLong();
      long inVertexId = unpacker.unpackLong();
      VertexRef outVertexRef = (VertexRef) graph.vertex(outVertexId);
      VertexRef inVertexRef = (VertexRef) graph.vertex(inVertexId);
      return edgeFactoryByLabel.get(label).createEdgeRef(id, graph, outVertexRef, inVertexRef);
    }
  }
}
