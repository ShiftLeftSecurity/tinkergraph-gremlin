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
      VertexSerializer serializer = new VertexSerializer();
      VertexDeserializer deserializer = newVertexDeserializer(graph);
      Vertex vertex = graph.addVertex(
          T.label, SerializerTestVertex.label,
          SerializerTestVertex.STRING_PROPERTY, "StringValue",
          SerializerTestVertex.INT_PROPERTY, 42,
          SerializerTestVertex.STRING_LIST_PROPERTY, Arrays.asList("stringOne", "stringTwo"),
          SerializerTestVertex.INT_LIST_PROPERTY, Arrays.asList(43, 44),
          SerializerTestVertex.OPTIONAL_LONG_PROPERTY, 45l
      );

      byte[] bytes = serializer.serialize(vertex);
      Vertex deserialized = deserializer.deserialize(bytes, true);

      Vertex underlyingVertexDb = ((VertexRef<TinkerVertex>) vertex).get();
      assertEquals(underlyingVertexDb, deserialized);

      final ElementRef<TinkerVertex> deserializedRef = deserializer.deserializeRef(bytes);
      assertEquals(vertex.id(), deserializedRef.id);
      assertEquals(SerializerTestVertex.label, deserializedRef.label());


      // test reading of specific properties
      SerializerTestVertex deserializedWithoutProps = (SerializerTestVertex) deserializer.deserialize(bytes, false);
      assertNull("property should not have been initialized", deserializedWithoutProps.stringProperty);
      assertNull("property should not have been initialized", deserializedWithoutProps.intProperty);
      assertNull("property should not have been initialized", deserializedWithoutProps.stringListProperty);
      assertNull("property should not have been initialized", deserializedWithoutProps.intListProperty);
      assertNull("property should not have been initialized", deserializedWithoutProps.optionalLongProperty);
      assertEquals("StringValue", deserializer.unpackSpecificProperty(bytes, SerializerTestVertex.STRING_PROPERTY_IDX, String.class));
      assertEquals(42, deserializer.unpackSpecificProperty(bytes, SerializerTestVertex.INT_PROPERTY_IDX, Integer.class));
      assertEquals(Arrays.asList("stringOne", "stringTwo"), deserializer.unpackSpecificProperty(bytes, SerializerTestVertex.STRING_LIST_PROPERTY_IDX, String.class));
      assertEquals(Arrays.asList(43, 44), deserializer.unpackSpecificProperty(bytes, SerializerTestVertex.INT_LIST_PROPERTY_IDX, Integer.class));
      assertEquals(45l, deserializer.unpackSpecificProperty(bytes, SerializerTestVertex.OPTIONAL_LONG_PROPERTY_IDX, Long.class));

      // simulate normal case where bytes are lazily read from storage
      graph.getOndiskOverflow().getVertexMVMap().put((Long) vertex.id(), bytes);
      assertEquals("StringValue", deserializedWithoutProps.value(SerializerTestVertex.STRING_PROPERTY));
      assertEquals(Arrays.asList(43, 44), deserializedWithoutProps.value(SerializerTestVertex.INT_LIST_PROPERTY));
    }
  }

  @Test
  public void serializeEdge() throws IOException {
    try (TinkerGraph graph = newGraph()) {
      EdgeSerializer serializer = new EdgeSerializer();
      EdgeDeserializer deserializer = newEdgeDeserializer(graph);

      Vertex v0 = graph.addVertex(T.label, SerializerTestVertex.label);
      Vertex v1 = graph.addVertex(T.label, SerializerTestVertex.label);
      Edge edge = v0.addEdge(SerializerTestEdge.label, v1, SerializerTestEdge.LONG_PROPERTY, Long.MAX_VALUE);

      byte[] bytes = serializer.serialize(edge);
      Edge deserialized = deserializer.deserialize(bytes, true);

      Edge underlyingEdgeDb = ((EdgeRef<TinkerEdge>) edge).get();
      assertEquals(underlyingEdgeDb, deserialized);

      final ElementRef<TinkerEdge> deserializedRef = deserializer.deserializeRef(bytes);
      assertEquals(edge.id(), deserializedRef.id);
      assertEquals(SerializerTestEdge.label, deserializedRef.label());

      // test reading of specific properties
      SerializerTestEdge deserializedWithoutProps = (SerializerTestEdge) deserializer.deserialize(bytes, false);
      assertNull("property should not have been initialized", deserializedWithoutProps.longProperty);
      assertEquals(Long.MAX_VALUE, deserializer.unpackSpecificProperty(bytes, SerializerTestEdge.LONG_PROPERTY_IDX, Long.class));

      // simulate normal case where bytes are lazily read from storage
      graph.getOndiskOverflow().getEdgeMVMap().put((Long) edge.id(), bytes);
      Long value = deserializedWithoutProps.value(SerializerTestEdge.LONG_PROPERTY); // triggers reading from storage
      assertEquals(Long.MAX_VALUE, value.longValue());
    }
  }

  @Test
  public void serializeVertexWithEdgeIds() throws IOException {
    try (TinkerGraph graph = newGraph()) {
      VertexSerializer serializer = new VertexSerializer();
      VertexDeserializer deserializer = newVertexDeserializer(graph);

      Vertex vertex0 = graph.addVertex(T.label, SerializerTestVertex.label);
      Vertex vertex1 = graph.addVertex(T.label, SerializerTestVertex.label);
      Edge edge0 = vertex0.addEdge(SerializerTestEdge.label, vertex1);
      Edge edge1 = vertex1.addEdge(SerializerTestEdge.label, vertex0);

      byte[] bytes = serializer.serialize(vertex0);
      Vertex deserialized = deserializer.deserialize(bytes, true);

      Vertex underlyingVertexDb = ((VertexRef<TinkerVertex>) vertex0).get();
      assertEquals(underlyingVertexDb, deserialized);
      assertEquals(edge0, deserialized.edges(Direction.OUT, SerializerTestEdge.label).next());
      assertEquals(edge1, deserialized.edges(Direction.IN, SerializerTestEdge.label).next());
      assertEquals(vertex1, deserialized.vertices(Direction.OUT, SerializerTestEdge.label).next());
    }
  }

  private VertexDeserializer newVertexDeserializer(TinkerGraph graph) {
    Map<String, SpecializedElementFactory.ForVertex> vertexFactories = new HashMap();
    vertexFactories.put(SerializerTestVertex.label, SerializerTestVertex.factory);
    return new VertexDeserializer(graph, vertexFactories);
  }

  private EdgeDeserializer newEdgeDeserializer(TinkerGraph graph) {
    Map<String, SpecializedElementFactory.ForEdge> edgeFactories = new HashMap();
    edgeFactories.put(SerializerTestEdge.label, SerializerTestEdge.factory);
    return new EdgeDeserializer(graph, edgeFactories);
  }

  private TinkerGraph newGraph() {
    return TinkerGraph.open(
        Arrays.asList(SerializerTestVertex.factory),
        Arrays.asList(SerializerTestEdge.factory)
    );
  }

}
