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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public class Artist extends SpecializedTinkerVertex {
    public static String label = "artist";

    public static String NAME = "name";
    public static Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(NAME));

    // properties
    private final String name;

    // edges
    public static String[] ALL_EDGES = new String[] {WrittenBy.label, SungBy.label};
    private Set<SungBy> sungByIn;
    private Set<WrittenBy> writtenByIn;

    public Artist(Object id, TinkerGraph graph, String name) {
        super(id, Artist.label, graph, SPECIFIC_KEYS);

        this.name = name;
    }

    /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
    @Override
    protected <V> V specificProperty(String key) {
        if (key == NAME) {
            return (V) name;
        } else {
            throw new NoSuchElementException(key);
        }
    }

    /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
    @Override
    protected Iterator<Edge> specificEdges(Direction direction, String... edgeLabels) {
        List<Iterator<?>> iterators = new LinkedList<>();
        if (edgeLabels.length == 0) {
            edgeLabels = ALL_EDGES;
        }
        for (String label : edgeLabels) {
            if (label == WrittenBy.label) {
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    iterators.add(getWrittenByIn().iterator());
                }
            } else if (label == SungBy.label) {
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    iterators.add(getSungByIn().iterator());
                }
            }
        }

        Iterator<Edge>[] iteratorsArray = iterators.toArray(new Iterator[iterators.size()]);
        return IteratorUtils.concat(iteratorsArray);
    }

    @Override
    protected void addSpecializedOutEdge(Edge edge) {
        throw new IllegalArgumentException("edge type " + edge.getClass() + " not supported");
    }

    @Override
    protected void addSpecializedInEdge(Edge edge) {
        if (edge instanceof WrittenBy) {
            getWrittenByIn().add((WrittenBy) edge);
        } else if (edge instanceof SungBy) {
            getSungByIn().add((SungBy) edge);
        } else {
            throw new IllegalArgumentException("edge type " + edge.getClass() + " not supported");
        }
    }

    private Set<WrittenBy> getWrittenByIn() {
        if (writtenByIn == null) {
            writtenByIn = new HashSet<>();
        }
        return writtenByIn;
    }

    private Set<SungBy> getSungByIn() {
        if (sungByIn == null) {
            sungByIn = new HashSet<>();
        }
        return sungByIn;
    }

    public static SpecializedElementFactory.ForVertex<Artist> factory = new SpecializedElementFactory.ForVertex<Artist>() {
        @Override
        public String forLabel() {
            return Artist.label;
        }

        @Override
        public Artist createVertex(Object id, TinkerGraph graph, Map<String, Object> keyValueMap) {
            String name = (String) keyValueMap.get("name");
            return new Artist(id, graph, name);
        }
    };
}
