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
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;

public class Artist extends SpecializedTinkerVertex {
    public static final String label = "artist";

    public static final String NAME = "name";
    public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(NAME));

    // properties
    private String name;

    // edges
    public static final String[] ALL_EDGES = new String[] {WrittenBy.label, SungBy.label};
    private Set<SungBy> sungByIn = new HashSet<>();
    private Set<Long> writtenByIn = new HashSet<>();

    public Artist(Long id, TinkerGraph graph) {
        super(id, Artist.label, graph, SPECIFIC_KEYS);
    }

    /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
    @Override
    protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
        final VertexProperty<V> ret;
        if (NAME.equals(key) && name != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, name));
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
        if (NAME.equals(key)) {
            this.name = (String) value;
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
        return property(key);
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
                    iterators.add(getWrittenByIn());
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
    protected void removeSpecificOutEdge(Edge edge) {
        throw new IllegalArgumentException("edge type " + edge.getClass() + " not supported");
    }

    @Override
    protected void removeSpecificInEdge(Edge edge) {
        if (edge instanceof WrittenBy) {
            if (writtenByIn != null) {
                writtenByIn.remove(edge.id());
            }
        } else if (edge instanceof SungBy) {
            if (sungByIn != null) {
                sungByIn.remove(edge);
            }
        } else {
            throw new IllegalArgumentException("edge type " + edge.getClass() + " not supported");
        }
    }

    @Override
    protected void addSpecializedOutEdge(Edge edge) {
        throw new IllegalArgumentException("edge type " + edge.getClass() + " not supported");
    }

    @Override
    protected void addSpecializedInEdge(Edge edge) {
        if (edge instanceof WrittenBy) {
            writtenByIn.add((Long) edge.id());
        } else if (edge instanceof SungBy) {
            getSungByIn().add((SungBy) edge);
        } else {
            throw new IllegalArgumentException("edge type " + edge.getClass() + " not supported");
        }
    }

    private Iterator<WrittenBy> getWrittenByIn() {
        return writtenByIn.stream().map(id -> (WrittenBy) graph.edgeById(id)).iterator();
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
        public Artist createVertex(long id, TinkerGraph graph) {
            return new Artist(id, graph);
        }
    };

    public String getName() {
        return name;
    }

}
