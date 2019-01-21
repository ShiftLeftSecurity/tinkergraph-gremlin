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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.VertexSerializer;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class StorageTest {

    @Test
    public void handleSerializeAndDeserializeVertex() throws IOException {
      TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
      VertexSerializer serializer = new VertexSerializer(graph, graph.specializedVertexFactoryByLabel);

      Artist garcia = (Artist) graph.traversal().V().has("name", "Garcia").next();
      byte[] bytes = serializer.serialize(garcia);

      Artist garcia2 = (Artist) serializer.deserialize(bytes);
      assertEquals(garcia.id(), garcia2.id());
      assertEquals(garcia.label(), garcia2.label());
      assertEquals(garcia.getName(), garcia2.getName());
    }

    @Test
    public void handleSerializeAndDeserializeEdge() throws IOException {
      TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//      DefaultEdgeSerializer serializer = new VertexSerializer(graph, graph.specializedVertexFactoryByLabel);
//
//      Vertex garcia = graph.traversal().V().has("name", "Garcia").next();
//      byte[] bytes = serializer.serialize((SpecializedTinkerVertex) garcia);
//
//      Vertex garcia2 = serializer.deserialize(bytes);
//      assertEquals(garcia.id(), garcia2.id());
//      assertEquals(garcia.label(), garcia2.label());
    }

    private TinkerGraph newGratefulDeadGraphWithSpecializedElements() {
        return TinkerGraph.open(
            Arrays.asList(Song.factory, Artist.factory),
            Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
        );
    }
    
    private TinkerGraph newGratefulDeadGraphWithSpecializedElementsWithData() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
        graph.io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
        return graph;
    }

}
