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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead.*;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class SpecializedElementsTest {

    @Test
    public void shouldSupportSpecializedElements() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();

        List<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toList();
        assertEquals(garcias.size(), 1);
        Artist garcia = (Artist) garcias.get(0); //it's actually of type `Artist`, not (only) `Vertex`
        assertEquals("Garcia", garcia.getName());

        List<Vertex> songsWritten = __(garcia).in(WrittenBy.label).toList();
        assertEquals(songsWritten.size(), 4);
        Song song = (Song) songsWritten.get(0); //it's actually of type `Artist`, not (only) `Vertex`
    }

    @Test
    public void shouldSupportRemovalOfSpecializedElements() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
        Set<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toSet();
        assertNotEquals(garcias.size(), 0);
        garcias.forEach(garcia -> garcia.remove());
        Long garciaCount = graph.traversal().V().has("name", "Garcia").count().next();
        assertEquals(garciaCount, Long.valueOf(0));

        List<Vertex> outVertices = graph.traversal().E().outV().toList();
        outVertices.forEach(outVertex -> assertFalse(garcias.contains(outVertex)));

        List<Vertex> inVertices = graph.traversal().E().inV().toList();
        inVertices.forEach(inVertex -> assertFalse(garcias.contains(inVertex)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowMixingWithGenericVertex() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
        graph.addVertex("something_else");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowMixingWithGenericEdge() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
        GraphTraversalSource g = graph.traversal();
        List<Vertex> vertices = g.V().limit(2).toList();
        Vertex v1 = vertices.get(0);
        Vertex v2 = vertices.get(1);
        v1.addEdge("something_else", v2);
    }

    @Test
    public void shouldUseIndices() throws IOException {
        int loops = 10000;
        Double avgTimeWithIndex = null;
        Double avgTimeWithoutIndex = null;

        { // tests with index
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            graph.createIndex("weight", Edge.class);
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
        }

        { // tests without index
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
        }

        System.out.println("avgTimeWithIndex = " + avgTimeWithIndex);
        System.out.println("avgTimeWithoutIndex = " + avgTimeWithoutIndex);
        assertTrue("avg time with index should be (significantly) less than without index",
            avgTimeWithIndex < avgTimeWithoutIndex);
    }
    
    @Test
    public void shouldUseIndicesCreatedBeforeLoadingData() throws IOException {
        int loops = 10000;
        Double avgTimeWithIndex = null;
        Double avgTimeWithoutIndex = null;

        { // tests with index
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
            graph.createIndex("weight", Edge.class);
            loadGraphMl(graph);
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
        }

        { // tests without index
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
        }

        System.out.println("avgTimeWithIndex = " + avgTimeWithIndex);
        System.out.println("avgTimeWithoutIndex = " + avgTimeWithoutIndex);
        assertTrue("avg time with index should be (significantly) less than without index",
            avgTimeWithIndex < avgTimeWithoutIndex);
    }

    @Test
    public void handleEmptyProperties() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();

        List<Object> props1 = graph.traversal().V().values("foo").toList();
        List<Object> props2 = graph.traversal().E().values("foo").toList();
        // results will be empty, but it should't crash. see https://github.com/ShiftLeftSecurity/tinkergraph-gremlin/issues/12
        assertEquals(props1.size(), 0);
        assertEquals(props2.size(), 0);
    }

    // @Test
    // only run manually since the timings vary depending on the environment
    public void propertyLookupPerformanceComparison() throws IOException {
        int loops = 1000;
        Double avgTimeWithSpecializedElements = null;
        Double avgTimeWithGenericElements = null;

        { // using specialized elements
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithSpecializedElements = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
        }

        { // using generic elements
            TinkerGraph graph = newGratefulDeadGraphWithGenericElementsWithData();
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithGenericElements = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
        }

        System.out.println("avgTimeWithSpecializedElements = " + avgTimeWithSpecializedElements);
        System.out.println("avgTimeWithGenericElements = " + avgTimeWithGenericElements);

        double diffPercent = (avgTimeWithGenericElements - avgTimeWithSpecializedElements) / avgTimeWithGenericElements;
        System.out.println("performance enhancement of specialized elements = " + diffPercent);

        assertTrue("avg time with specialized elements should be less than with generic elements",
            avgTimeWithSpecializedElements < avgTimeWithGenericElements);
    }

//    @Test
    // only run manually since the timings vary depending on the environment
    public void traversalPerformanceComparison() throws IOException {
        int loops = 100;
        Double avgTimeWithSpecializedElements = null;
        Double avgTimeWithGenericElements = null;

        { // using specialized elements
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            GraphTraversalSource g = graph.traversal();
            avgTimeWithSpecializedElements = TimeUtil.clock(loops, () -> g.V().out().out().out().toStream().count());
        }

        { // using generic elements
            TinkerGraph graph = newGratefulDeadGraphWithGenericElementsWithData();
            GraphTraversalSource g = graph.traversal();
            avgTimeWithGenericElements = TimeUtil.clock(loops, () -> g.V().out().out().out().toStream().count());
        }

        System.out.println("avgTimeWithSpecializedElements = " + avgTimeWithSpecializedElements);
        System.out.println("avgTimeWithGenericElements = " + avgTimeWithGenericElements);

        double diffPercent = (avgTimeWithGenericElements - avgTimeWithSpecializedElements) / avgTimeWithGenericElements;
        System.out.println("performance enhancement of specialized elements = " + diffPercent);
    }

    private TinkerGraph newGratefulDeadGraphWithSpecializedElements() throws IOException {
        return TinkerGraph.open(
            Arrays.asList(Song.factory, Artist.factory),
            Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
        );
    }
    
    private TinkerGraph newGratefulDeadGraphWithSpecializedElementsWithData() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
        loadGraphMl(graph);
        return graph;
    }

    private TinkerGraph newGratefulDeadGraphWithGenericElementsWithData() throws IOException {
        TinkerGraph graph = TinkerGraph.open();
        loadGraphMl(graph);
        return graph;
    }


    private void loadGraphMl(TinkerGraph graph) throws IOException {
        graph.io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
    }

}
