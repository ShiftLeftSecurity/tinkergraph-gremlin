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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VertexSerializer extends Serializer<Vertex> {

  @Override
  protected long getId(Vertex vertex) {
    return (long) vertex.id();
  }

  @Override
  protected String getLabel(Vertex vertex) {
    return vertex.label();
  }

  @Override
  protected Map<String, Object> getProperties(Vertex vertex) {
    if (vertex instanceof SpecializedTinkerVertex) {
      return ((SpecializedTinkerVertex) vertex).valueMap();
    } else {
      Map<String, Object> properties = new HashMap<>();
      vertex.properties().forEachRemaining(property -> properties.put(property.key(), property.value()));
      return properties;
    }
  }

  @Override
  protected Map<String, Set<Long>> getEdgeIds(Vertex vertex, Direction direction) {
    Map<String, Set<Long>> edgeIdsByLabel = new HashMap<>();
    vertex.edges(direction).forEachRemaining(edge -> {
      edgeIdsByLabel.computeIfAbsent(edge.label(), label -> new HashSet<>());
      edgeIdsByLabel.get(edge.label()).add((Long) edge.id());
    });
    return edgeIdsByLabel;
  }

}
