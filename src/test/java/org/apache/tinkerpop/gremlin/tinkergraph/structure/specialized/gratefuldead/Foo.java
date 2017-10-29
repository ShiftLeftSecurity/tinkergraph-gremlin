package org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead;/*
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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.TimeUtil;

import java.io.IOException;

public class Foo {

    public static void main(String[] args) throws IOException {
        TinkerGraph graph = TinkerGraph.open();
        graph.registerSpecializedVertexFactory(Song.factory);
        graph.registerSpecializedVertexFactory(Artist.factory);
        graph.registerSpecializedEdgeFactory(FollowedBy.factory);
        graph.registerSpecializedEdgeFactory(SungBy.factory);
        graph.registerSpecializedEdgeFactory(WrittenBy.factory);

//        graph.createIndex("weight", Edge.class);



        graph.io(IoCore.graphml()).readGraph("data/grateful-dead.xml");
//        System.out.println(graph.specializedVertexCount());
//        System.out.println(graph.specializedEdgeCount());

        GraphTraversalSource g = graph.traversal();

        int loops = 10000;
//        System.out.println(TimeUtil.clockWithResult(loops, () -> g.V().out().out().out().toStream().count()));
//        System.out.println(TimeUtil.clockWithResult(loops, () -> g.V().out().out().out().valueMap(true).toStream().count()));

        System.out.println(TimeUtil.clockWithResult(loops, () -> g.E().has("weight", P.eq(1)).toStream().count()));
        // no index, generic: 1.33
        // with index, generic: 0.64
        // no index, specific: 0.114
        // with index, specific: 0.055



//        g.V().outE().count().toList().forEach(v -> System.out.println(v));

//        g.V().valueMap().toList().forEach(v -> System.out.println(v));
//        g.E().valueMap().toList().forEach(v -> System.out.println(v));
//
//        System.out.println(g.V(from.id()).outE(FollowedBy.label).toList());
//        System.out.println(g.V(from.id()).outE(FollowedBy.label).inV().toList());
//        System.out.println(g.V(from.id()).out(FollowedBy.label).toList());
//
//        System.out.println(g.V(from.id()).bothE(FollowedBy.label).toList());
//        System.out.println(g.V(from.id()).both(FollowedBy.label).toList());
    }
}