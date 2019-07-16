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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.ElementRef;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowDbNode;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import java.util.Map;

public class VertexDeserializer extends Deserializer<Vertex> {
  protected final TinkerGraph graph;
  protected final Map<String, OverflowElementFactory.ForVertex> vertexFactoryByLabel;

  public VertexDeserializer(TinkerGraph graph, Map<String, OverflowElementFactory.ForVertex> vertexFactoryByLabel) {
    this.graph = graph;
    this.vertexFactoryByLabel = vertexFactoryByLabel;
  }

  @Override
  protected TinkerGraph graph() {
    return graph;
  }

  @Override
  protected ElementRef createNodeRef(long id, String label) {
    OverflowElementFactory.ForVertex vertexFactory = vertexFactoryByLabel.get(label);
    if (vertexFactory == null) {
      throw new AssertionError("vertexFactory not found for label=" + label);
    }

    return vertexFactory.createVertexRef(id, graph);
  }

  @Override
  protected Vertex createNode(long id, String label, Map<String, Object> properties, int[] edgeOffsets, Object[] adjacentVerticesWithProperties) {
    OverflowElementFactory.ForVertex vertexFactory = vertexFactoryByLabel.get(label);
    if (vertexFactory == null) {
      throw new AssertionError("vertexFactory not found for label=" + label);
    }
    OverflowDbNode vertex = vertexFactory.createVertex(id, graph);
    ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, toTinkerpopKeyValues(properties));
    vertex.setEdgeOffsets(edgeOffsets);
    vertex.setAdjacentVerticesWithProperties(adjacentVerticesWithProperties);

    return vertex;
  }

}
