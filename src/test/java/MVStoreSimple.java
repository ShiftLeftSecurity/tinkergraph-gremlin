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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead.*;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MVStoreSimple {

  public static void main(String[] args) throws IOException {
//    runSimple(true);
//    runSimple(false);

    runNodes(true);
  }

  private static void runNodes(boolean insert) throws IOException {
    String fileName = "mvstore2.bin";

    if (insert) {
      MVStore s = new MVStore.Builder().fileName(fileName).open();
      TinkerGraph graph = newGratefulDeadGraphWithSpecializedElementsWithData();
      List<Vertex> garcias = graph.traversal().V().has("name", "Garcia").toList();
      Vertex garcia = garcias.get(0);
      System.out.println(garcia);
      System.out.println(garcia.getClass());
      MVMap<Long, Vertex> map = s.openMap("data");
      map.put(1l, garcia);
      s.close();
    } else {
//      MVStore s = new MVStore.Builder().fileName(fileName).readOnly().open();
//      MVMap<Integer, String> map = s.openMap("data");
//      map.put(1, "Hello World");
//      System.out.println(map.get(1));
//      s.close();
    }
  }

  private static void runSimple(boolean insert) {
    String fileName = "mvstore.bin";

    if (insert) {
      MVStore s = new MVStore.Builder().fileName(fileName).open();
      MVMap<Integer, String> map = s.openMap("data");
      map.put(1, "Hello World");
      s.close();
    } else {
      MVStore s = new MVStore.Builder().fileName(fileName).readOnly().open();

      MVMap<Integer, String> map = s.openMap("data");
      map.put(1, "Hello World");
      System.out.println(map.get(1));
      s.close();
    }
  }

  private static TinkerGraph newGratefulDeadGraphWithSpecializedElements() {
    return TinkerGraph.open(
      Arrays.asList(Song.factory, Artist.factory),
      Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
    );
  }

  private static TinkerGraph newGratefulDeadGraphWithSpecializedElementsWithData() throws IOException {
    TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements();
    loadGraphMl(graph);
    return graph;
  }

  private static void loadGraphMl(TinkerGraph graph) throws IOException {
    graph.io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
  }
}
