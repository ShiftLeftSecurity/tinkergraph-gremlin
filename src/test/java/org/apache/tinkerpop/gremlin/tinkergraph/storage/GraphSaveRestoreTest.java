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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

/** save and restore a graph from disk overlay */
public class GraphSaveRestoreTest {

    @Test
    public void greenField() throws IOException {
        final File overflowDb = Files.createTempFile("overflowdb", "bin").toFile();
        overflowDb.deleteOnExit();

        TinkerGraph graph = newGratefulDeadGraphWithSpecializedElements(overflowDb);
        Vertex v0 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 1");
        Vertex v2 = graph.addVertex(T.label, Song.label, Song.NAME, "Song 2");
        v0.addEdge(FollowedBy.label, v2);

        graph.close(); // will trigger saving to disk because we specified a location
        // TODO reload from disk
    }

    private TinkerGraph newGratefulDeadGraphWithSpecializedElements(File overflowDb) {
        Configuration configuration = TinkerGraph.EMPTY_CONFIGURATION();
        configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_ENABLED, true);
        configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, overflowDb.getAbsolutePath());
        return TinkerGraph.open(
            configuration,
            Arrays.asList(Song.factory, Artist.factory),
            Arrays.asList(FollowedBy.factory, SungBy.factory, WrittenBy.factory)
        );
    }
    
    private void loadGraphMl(TinkerGraph graph) throws IOException {
        graph.io(IoCore.graphml()).readGraph("src/test/resources/grateful-dead.xml");
    }

}
