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

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EdgeSerializer extends Serializer<Edge> {

  @Override
  protected long getId(Edge edge) {
    return (long) edge.id();
  }

  @Override
  protected String getLabel(Edge edge) {
    return edge.label();
  }

  @Override
  protected Map<String, Object> getProperties(Edge edge) {
    Map<String, Object> properties = new HashMap<>();
    edge.properties().forEachRemaining(property -> properties.put(property.key(), property.value()));
    return properties;
  }

  @Override
  /** using same format to store edgeIds as for vertices */
  protected Map<String, Set<Long>> getEdgeIds(Edge edge, Direction direction) {
    final Map<String, Set<Long>> edgeIds = new HashMap<>();
    switch (direction) {
      case IN:
        edgeIds.put(Direction.IN.name(), new HashSet<>(Arrays.asList((Long) edge.inVertex().id())));
        break;
      case OUT:
        edgeIds.put(Direction.OUT.name(), new HashSet<>(Arrays.asList((Long) edge.outVertex().id())));
        break;
      default: throw new NotImplementedException();
    }
    return edgeIds;
  }

}
