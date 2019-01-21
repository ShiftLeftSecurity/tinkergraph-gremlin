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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.Serializer;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class DiskStorageTest {

    @Test
    public void foo() throws IOException {
        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
//        Vertex garcia = graph.traversal().V().has("name", "Garcia").next();
//
//        byte[] bytes = vertexSerializer.serialize(garcia);
//        System.out.println(vertexSerializer.deserialize(bytes));

//        graph.traversal().V().toList().forEach((Vertex v) -> {
//            try {
//                byte[] bytes = vertexSerializer.serialize(v);
//                System.out.println(vertexSerializer.deserialize(bytes).getClass());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });

    }


    private TinkerGraph newGratefulDeadGraphWithSpecializedElements() {
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


    private void loadGraphMl(TinkerGraph graph) throws IOException {
        graph.io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
    }

//    private ObjectMapper mapper = new ObjectMapper();

    /*
    * to be able to decode into the correct type, we're encoding the type into the serialized value
    * this is fairly silly, but ok for the purpose of a test
    * */
//    private Serializer<Vertex> vertexSerializer = new Serializer<Vertex>() {
//
//        @Override
//        public byte[] serialize(Vertex tinkerVertex) throws JsonProcessingException {
//            byte encodedType;
//            switch (tinkerVertex.label()) {
//                case Artist.label:
//                    encodedType = 1;
//                    break;
//                case Song.label:
//                    encodedType = 2;
//                    break;
//                default:
//                    throw new AssertionError("unknown label: " + tinkerVertex.label());
//            }
//
//            System.out.println(mapper.writeValueAsString(tinkerVertex));
//            byte[] serialized = mapper.writeValueAsBytes(tinkerVertex);
//            byte[] res = new byte[serialized.length + 1];
//            res[0] = encodedType;
//            for (int i = 0; i < serialized.length; i++) {
//                res[i+1] = serialized[i];
//            }
//            return res;
//        }
//
//        @Override
//        public Vertex deserialize(byte[] bytes) throws IOException {
//            byte encodedType = bytes[0];
//            Class<? extends Vertex> targetClass = null;
//            switch (encodedType) {
//                case 1:
//                    targetClass = Artist.class;
//                    break;
//                case 2:
//                    targetClass = Song.class;
//                    break;
//                default:
//                    throw new AssertionError("unknown encodedType: " + encodedType);
//            }
////            Arrays.copyOf()
//
//            byte[] withoutEncodedType = new byte[bytes.length - 1];
//            System.arraycopy(bytes, 1, withoutEncodedType, 0, withoutEncodedType.length);
//
////            System.out.println("deserialize");
////            System.out.println(withoutEncodedType.length);
////            System.out.println(withoutEncodedType[0]);
////            System.out.println(withoutEncodedType[1]);
////            System.out.println(withoutEncodedType[2]);
//
//            return mapper.readValue(withoutEncodedType, targetClass);
//        }
//    };
//
//    private Serializer<Edge> edgeSerializer = null;

}
