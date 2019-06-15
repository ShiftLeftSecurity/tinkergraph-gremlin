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
        TinkerGraph graph = newGraph();
        VertexSerializer serializer = newVertexSerializer(graph);

        Vertex vertex = graph.addVertex(
            T.label, SerializerTestVertex.label,
            SerializerTestVertex.STRING_PROPERTY, "StringValue",
            SerializerTestVertex.INT_PROPERTY, Integer.MAX_VALUE
            );

        byte[] bytes = serializer.serialize(vertex);
        TinkerVertex deserialized = serializer.deserialize(bytes);

        Vertex underlyingVertexDb = ((VertexRef<TinkerVertex>) vertex).get();
        assertEquals(underlyingVertexDb, deserialized);
//        deserialized.values().forEachRemaining(System.out::println);

        graph.close();
    }

    private VertexSerializer newVertexSerializer(TinkerGraph graph) {
        Map<String, SpecializedElementFactory.ForVertex> vertexFactories = new HashMap();
        vertexFactories.put(SerializerTestVertex.label, SerializerTestVertex.factory);
        return new VertexSerializer(graph, vertexFactories);
    }


    private TinkerGraph newGraph() {
        return TinkerGraph.open(
            Arrays.asList(SerializerTestVertex.factory),
            Arrays.asList()
        );
    }



}
