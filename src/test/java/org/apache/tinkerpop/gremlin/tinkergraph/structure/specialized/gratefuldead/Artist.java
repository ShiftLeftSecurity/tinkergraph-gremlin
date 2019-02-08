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

import org.apache.commons.lang3.NotImplementedException;
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
    private Set<Long> sungByIn = new HashSet<>();
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

    @Override
    protected void removeSpecificProperty(String key) {
        if (NAME.equals(key)) {
            this.name = null;
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
    }

    /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
    @Override
    protected Iterator<Long> specificEdges(Direction direction, String... edgeLabels) {
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
                    iterators.add(getSungByIn());
                }
            }
        }

        Iterator<Long>[] iteratorsArray = iterators.toArray(new Iterator[iterators.size()]);
        return IteratorUtils.concat(iteratorsArray);
    }

    @Override
    protected void removeSpecificOutEdge(Long edgeId) {
        throw new IllegalArgumentException("no out edges allowed here");
    }

    @Override
    protected void removeSpecificInEdge(Long edgeId) {
        writtenByIn.remove(edgeId);
        sungByIn.remove(edgeId);
    }

    @Override
    public Map<String, Set<Long>> edgeIdsByLabel(Direction direction) {
        final Map<String, Set<Long>> result = new HashMap<>();
        if (direction.equals(Direction.IN)) {
            result.put(SungBy.label, sungByIn);
            result.put(WrittenBy.label, writtenByIn);
        } else if (direction.equals(Direction.OUT)) {
        } else {
            throw new NotImplementedException("not implemented for direction=" + direction);
        }

        return result;
    }

    @Override
    public void addSpecializedOutEdge(String edgeLabel, long edgeId) {
        throw new IllegalArgumentException("no out edges allowed here");
    }

    @Override
    public void addSpecializedInEdge(String edgeLabel, long edgeId) {
        if (WrittenBy.label.equals(edgeLabel)) {
            writtenByIn.add(edgeId);
        } else if (SungBy.label.equals(edgeLabel)) {
            sungByIn.add(edgeId);
        } else {
            throw new IllegalArgumentException("edge type " + edgeLabel + " not supported");
        }
    }

    private Iterator<Long> getWrittenByIn() {
        return writtenByIn.iterator();
    }

    private Iterator<Long> getSungByIn() {
        return sungByIn.iterator();
    }

    public static SpecializedElementFactory.ForVertex<Artist> factory = new SpecializedElementFactory.ForVertex<Artist>() {
        @Override
        public String forLabel() {
            return Artist.label;
        }

        @Override
        public Artist createVertex(Long id, TinkerGraph graph) {
            return new Artist(id, graph);
        }
    };

    public String getName() {
        return name;
    }

}
