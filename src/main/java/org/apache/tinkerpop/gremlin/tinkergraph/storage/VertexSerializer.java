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

import gnu.trove.map.hash.THashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.ElementRef;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRef;

import java.util.Map;
import java.util.SortedMap;

public class VertexSerializer extends Serializer<Vertex> {

  @Override
  protected long getId(Vertex vertex) {
    return (long) vertex.id();
  }

  @Override
  protected String getLabel(Vertex vertex) {
    return vertex.label();
  }

  /**
   * Map<PropertyIndex, PropertyValue>, sorted by it's index so we can write it efficiently
   */
  @Override
  protected SortedMap<Integer, Object> getProperties(Vertex vertex) {
    if (vertex instanceof ElementRef) {
      vertex = ((ElementRef<TinkerVertex>) vertex).get();
    }
    if (vertex instanceof SpecializedTinkerVertex) {
      return ((SpecializedTinkerVertex) vertex).propertiesByStorageIdx();
    } else {
      throw new NotImplementedException("VertexSerializer.getProperties for generic vertices");
    }
  }

  @Override
  protected Map<String, TLongSet> getEdgeIds(Vertex vertex, Direction direction) {
    Map<String, TLongSet> edgeIdsByLabel = new THashMap<>();
    vertex.edges(direction).forEachRemaining(edge -> {
      edgeIdsByLabel.computeIfAbsent(edge.label(), label -> new TLongHashSet());
      edgeIdsByLabel.get(edge.label()).add((long) edge.id());
    });
    return edgeIdsByLabel;
  }

}
