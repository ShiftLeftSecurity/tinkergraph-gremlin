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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead.*;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;

public class SpecializedElementsTest {

    @Test
    public void shouldSupportSpecializedElements() throws IOException {
        final TinkerGraph graph = TinkerGraph.open();
        graph.registerSpecializedVertexFactory(Song.factory);
        graph.registerSpecializedVertexFactory(Artist.factory);
        graph.registerSpecializedEdgeFactory(FollowedBy.factory);
        graph.registerSpecializedEdgeFactory(SungBy.factory);
        graph.registerSpecializedEdgeFactory(WrittenBy.factory);

        graph.io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
        GraphTraversalSource g = graph.traversal();

        List<Vertex> garcias = g.V().has("name", "Garcia").toList();
        assertEquals(garcias.size(), 1);
        Artist garcia = (Artist) garcias.get(0); //it's actually of type `Artist`, not (only) `Vertex`
        assertEquals("Garcia", garcia.getName());

        List<Vertex> songsWritten = __(garcia).in(WrittenBy.label).toList();
        assertEquals(songsWritten.size(), 4);
        Song song = (Song) songsWritten.get(0); //it's actually of type `Artist`, not (only) `Vertex`
    }


}
