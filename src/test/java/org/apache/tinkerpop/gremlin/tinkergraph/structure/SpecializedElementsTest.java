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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead.*;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.*;

public class SpecializedElementsTest {

    @Test
    public void shouldSupportSpecializedElements() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();

        List<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toList();
        assertEquals(garcias.size(), 1);
        Artist garcia = (Artist) garcias.get(0); //it's actually of type `Artist`, not (only) `Vertex`
        assertEquals("Garcia", garcia.getName());
        graph.close();
    }

    @Test
    public void testBasicSteps() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
        Vertex garcia = graph.traversal().V().has("name", "Garcia").next();

        // inE
        assertEquals(4, __(garcia).inE(WrittenBy.label).toList().size());

        // in
        List<Vertex> songsWritten = __(garcia).in(WrittenBy.label).has("name", "CREAM PUFF WAR").toList();
        assertEquals(songsWritten.size(), 1);
        Song song = (Song) songsWritten.get(0); //it's actually of type `Artist`, not (only) `Vertex`
        assertEquals("CREAM PUFF WAR", song.getName());

        // outE
        assertEquals(1, __(song).outE(WrittenBy.label).toList().size());

        // out
        List<Vertex> songOut = __(song).out().toList();
        assertEquals(1, songOut.size());
        assertEquals(garcia, songOut.get(0));

        // bothE
        List<Edge> songBothE = __(song).bothE().toList();
        assertEquals(1, songBothE.size());

        // both
        List<Vertex> songBoth = __(song).both().toList();
        assertEquals(1, songBoth.size());
        assertEquals(garcia, songBoth.get(0));
        graph.close();
    }

    @Test
    public void shouldSupportRemovalOfSpecializedElements() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
        Set<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toSet();
        assertNotEquals(0, garcias.size());
        garcias.forEach(garcia -> garcia.remove());
        Long garciaCount = graph.traversal().V().has("name", "Garcia").count().next();
        assertEquals(Long.valueOf(0), garciaCount);

        List<Vertex> outVertices = graph.traversal().E().outV().toList();
        outVertices.forEach(outVertex -> assertFalse(garcias.contains(outVertex)));

        List<Vertex> inVertices = graph.traversal().E().inV().toList();
        inVertices.forEach(inVertex -> assertFalse(garcias.contains(inVertex)));
        graph.close();
    }

    @Test
    public void shouldNotAllowMixingWithGenericVertex() throws IOException {
        boolean caughtException = false;
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
        try {
            graph.addVertex("something_else");
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        graph.close();
        assertTrue(caughtException);
    }

    @Test
    public void shouldNotAllowMixingWithGenericEdge() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
        boolean caughtException = false;
        try {
            GraphTraversalSource g = graph.traversal();
            List<Vertex> vertices = g.V().limit(2).toList();
            Vertex v1 = vertices.get(0);
            Vertex v2 = vertices.get(1);
            v1.addEdge("something_else", v2);
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        graph.close();
        assertTrue(caughtException);
    }

    @Test
    public void shouldUseIndices() throws IOException {
        int loops = 100;
        Double avgTimeWithIndex = null;
        Double avgTimeWithoutIndex = null;

        { // tests with index
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            graph.createIndex("weight", Edge.class);
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
            graph.close();
        }

        { // tests without index
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
            graph.close();
        }

        System.out.println("avgTimeWithIndex = " + avgTimeWithIndex);
        System.out.println("avgTimeWithoutIndex = " + avgTimeWithoutIndex);
        assertTrue("avg time with index should be (significantly) less than without index",
            avgTimeWithIndex < avgTimeWithoutIndex);
    }
    
    @Test
    public void shouldUseIndicesCreatedBeforeLoadingData() throws IOException {
        int loops = 100;
        Double avgTimeWithIndex = null;
        Double avgTimeWithoutIndex = null;

        { // tests with index
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
            graph.createIndex("weight", Edge.class);
            loadGraphMl(graph);
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
            graph.close();
        }

        { // tests without index
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithoutIndex = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
            graph.close();
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
        graph.close();
    }

    // @Test
    // only run manually since the timings vary depending on the environment
    public void propertyLookupPerformanceComparison() throws IOException {
        int loops = 100;
        Double avgTimeWithSpecializedElements = null;
        Double avgTimeWithGenericElements = null;

        { // using specialized elements
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithSpecializedElements = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
            graph.close();
        }

        { // using generic elements
            TinkerGraph graph = newGratefulDeadGraphWithGenericElementsWithData();
            GraphTraversalSource g = graph.traversal();
            assertEquals(3564, (long) g.E().has("weight", P.eq(1)).count().next());
            avgTimeWithGenericElements = TimeUtil.clock(loops, () -> g.E().has("weight", P.eq(1)).count().next());
            graph.close();
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
        int loops = 10;
        Double avgTimeWithSpecializedElements = null;
        Double avgTimeWithGenericElements = null;

        { // using specialized elements
            TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
            GraphTraversalSource g = graph.traversal();
            avgTimeWithSpecializedElements = TimeUtil.clock(loops, () -> g.V().out().out().out().toStream().count());
            graph.close();
        }

        { // using generic elements
            TinkerGraph graph = newGratefulDeadGraphWithGenericElementsWithData();
            GraphTraversalSource g = graph.traversal();
            avgTimeWithGenericElements = TimeUtil.clock(loops, () -> g.V().out().out().out().toStream().count());
            graph.close();
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
