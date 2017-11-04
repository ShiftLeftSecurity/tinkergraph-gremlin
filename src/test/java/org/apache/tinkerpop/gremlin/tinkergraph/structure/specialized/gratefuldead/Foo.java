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
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.TimeUtil;

import java.io.IOException;
import java.util.Iterator;

public class Foo {

    public static void main(String[] args) throws IOException {
        TinkerGraph graph = TinkerGraph.open();
        graph.registerSpecializedVertexFactory(Song.factory);
        graph.registerSpecializedVertexFactory(Artist.factory);
        graph.registerSpecializedEdgeFactory(FollowedBy.factory);
        graph.registerSpecializedEdgeFactory(SungBy.factory);
        graph.registerSpecializedEdgeFactory(WrittenBy.factory);


//
//        graph.createIndex("weight", Edge.class);

        graph.io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
//        System.out.println(graph.specializedVertexCount());
//        System.out.println(graph.specializedEdgeCount());

        GraphTraversalSource g = graph.traversal();

        Iterator<Property<Object>> iter =
            g.E().next().properties();
//            g.E().next().properties("weight", "ASD");
        System.out.println(iter.hasNext());
        System.out.println(iter.next());
        System.out.println(iter.hasNext());

//        int loops = 10000;
//        System.out.println(TimeUtil.clockWithResult(loops, () -> g.V().out().out().out().toStream().count()));
//        System.out.println(TimeUtil.clockWithResult(loops, () -> g.V().out().out().out().valueMap(true).toStream().count()));

//        System.out.println(TimeUtil.clockWithResult(loops, () -> g.E().has("weight", P.eq(1)).toStream().count()));
        // no index, generic: 1.33
        // with index, generic: 0.64
        // no index, specific: 0.114
        // with index, specific: 0.055

//        Iterator<VertexProperty<Object>> iter =
//            g.V("1").next().properties("name", "asd");
//        System.out.println(iter.hasNext());
//        System.out.println(iter.next());
//        System.out.println(iter.hasNext());

//        System.out.println(g.V().values("name").count().toList());
//        System.out.println(g.V().values("bla").count().toList());
//        System.out.println(g.V().values("bla").toList());
//        System.out.println(g.V("1").next().properties("name").hasNext());
//        System.out.println(g.V("1").next().properties("asfd").hasNext());


        /* property lookup speed comparison
         * 50k loops: specialized: 19s, non-specialized: 7.5s, specialized without overriding property lookup: 18s, specialized, fixed: 8s
         * 100k loops: specialized: 36s, non-specialized: 14s
         * */
//        int loops = 50000;
//        long start = System.currentTimeMillis();
//        System.out.println(TimeUtil.clockWithResult(loops, () -> g.V().values("name").count().toList()));
//        long totalTime = System.currentTimeMillis() - start;
//        System.out.println("total time: " + totalTime);
        /* property lookup speed comparison end */

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