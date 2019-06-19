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

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class SerializerTest {

  @Test
  public void serializeVertex() throws IOException {
    try (TinkerGraph graph = newGraph()) {
      VertexSerializer serializer = newVertexSerializer(graph);
      Vertex vertex = graph.addVertex(
          T.label, SerializerTestVertex.label,
          SerializerTestVertex.STRING_PROPERTY, "StringValue",
          SerializerTestVertex.INT_PROPERTY, Integer.MAX_VALUE,
          SerializerTestVertex.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          SerializerTestVertex.INT_LIST_PROPERTY, Arrays.asList(42, 43)
      );

      byte[] bytes = serializer.serialize(vertex);
      TinkerVertex deserialized = serializer.deserialize(bytes);

      Vertex underlyingVertexDb = ((VertexRef<TinkerVertex>) vertex).get();
      assertEquals(underlyingVertexDb, deserialized);

      final ElementRef<TinkerVertex> deserializedRef = serializer.deserializeRef(bytes);
      assertEquals(vertex.id(), deserializedRef.id);
      assertEquals(SerializerTestVertex.label, deserializedRef.label());
    }
  }

  @Test
  public void serializeEdge() throws IOException {
    try (TinkerGraph graph = newGraph()) {
      EdgeSerializer serializer = newEdgeSerializer(graph);

      Vertex v0 = graph.addVertex(T.label, SerializerTestVertex.label);
      Vertex v1 = graph.addVertex(T.label, SerializerTestVertex.label);
      Edge edge = v0.addEdge(SerializerTestEdge.label, v1, SerializerTestEdge.LONG_PROPERTY, Long.MAX_VALUE);

      byte[] bytes = serializer.serialize(edge);
      TinkerEdge deserialized = serializer.deserialize(bytes);

      Edge underlyingEdgeDb = ((EdgeRef<TinkerEdge>) edge).get();
      assertEquals(underlyingEdgeDb, deserialized);

      final ElementRef<TinkerEdge> deserializedRef = serializer.deserializeRef(bytes);
      assertEquals(edge.id(), deserializedRef.id);
      assertEquals(SerializerTestEdge.label, deserializedRef.label());
    }
  }

  @Test
  public void serializeVertexWithEdgeIds() throws IOException {
    try (TinkerGraph graph = newGraph()) {
      VertexSerializer serializer = newVertexSerializer(graph);

      Vertex vertex0 = graph.addVertex(T.label, SerializerTestVertex.label);
      Vertex vertex1 = graph.addVertex(T.label, SerializerTestVertex.label);
      Edge edge0 = vertex0.addEdge(SerializerTestEdge.label, vertex1);
      Edge edge1 = vertex1.addEdge(SerializerTestEdge.label, vertex0);

      byte[] bytes = serializer.serialize(vertex0);
      TinkerVertex deserialized = serializer.deserialize(bytes);

      Vertex underlyingVertexDb = ((VertexRef<TinkerVertex>) vertex0).get();
      assertEquals(underlyingVertexDb, deserialized);
      assertEquals(edge0, deserialized.edges(Direction.OUT, SerializerTestEdge.label).next());
      assertEquals(edge1, deserialized.edges(Direction.IN, SerializerTestEdge.label).next());
      assertEquals(vertex1, deserialized.vertices(Direction.OUT, SerializerTestEdge.label).next());
    }
  }

  private VertexSerializer newVertexSerializer(TinkerGraph graph) {
    Map<String, SpecializedElementFactory.ForVertex> vertexFactories = new HashMap();
    vertexFactories.put(SerializerTestVertex.label, SerializerTestVertex.factory);
    return new VertexSerializer(graph, vertexFactories);
  }

  private EdgeSerializer newEdgeSerializer(TinkerGraph graph) {
    Map<String, SpecializedElementFactory.ForEdge> edgeFactories = new HashMap();
    edgeFactories.put(SerializerTestEdge.label, SerializerTestEdge.factory);
    return new EdgeSerializer(graph, edgeFactories);
  }

  private TinkerGraph newGraph() {
    return TinkerGraph.open(
        Arrays.asList(SerializerTestVertex.factory),
        Arrays.asList(SerializerTestEdge.factory)
    );
  }

}
