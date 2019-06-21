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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class VertexSerializer extends Serializer<Vertex> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private int serializedCount = 0;
  private long serializationTimeSpentMillis = 0;

  @Override
  public byte[] serialize(Vertex vertex) throws IOException {
    if (vertex instanceof VertexRef) { // unwrap
      vertex = ((VertexRef<TinkerVertex>) vertex).get();
    }

    long start = System.currentTimeMillis();
    try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
//    ((SpecializedTinkerVertex) vertex).acquireModificationLock();
      packer.packLong((Long) vertex.id());
      packer.packString(vertex.label());

      if (vertex instanceof SpecializedTinkerVertex) {
        // optimization for better performance
        packProperties(packer, ((SpecializedTinkerVertex) vertex).valueMap());
      } else {
        packProperties(packer, vertex.properties());
      }
      packEdgeIds(packer, vertex);
      //    ((SpecializedTinkerVertex) vertex).releaseModificationLock();

      serializedCount++;
      serializationTimeSpentMillis += System.currentTimeMillis() - start;
      if (serializedCount % 100000 == 0) {
        float avgSerializationTime = serializationTimeSpentMillis / (float) serializedCount;
        logger.debug("stats: serialized " + serializedCount + " vertices in total (avg time: " + avgSerializationTime + "ms)");
      }
      return packer.toByteArray();
    }
  }

  /**
   * format: two `Map<Label, Array<EdgeId>>`, i.e. one Map for `IN` and one for `OUT` edges
   */
  private void packEdgeIds(MessageBufferPacker packer, Vertex vertex) throws IOException {
    for (Direction direction : new Direction[]{Direction.IN, Direction.OUT}) {
      // note: this doesn't invoke edge deserialization, because both `id` and `label` are available in `EdgeRef`
      List<Edge> edges = IteratorUtils.toList(vertex.edges(direction));
      // a simple group by would be nice, but java collections are still very basic apparently
      Set<String> labels = edges.stream().map(e -> e.label()).collect(Collectors.toSet());
      packer.packMapHeader(labels.size());

      for (String label : labels) {
        packer.packString(label);
        Set<Long> edgeIds = edges.stream().filter(e -> e.label().equals(label)).map(e -> (Long) e.id()).collect(Collectors.toSet());
        packer.packArrayHeader(edgeIds.size());
        for (Long edgeId : edgeIds) {
          packer.packLong(edgeId);
        }
      }
    }
  }

}
