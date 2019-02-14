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
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;

public class Song extends SpecializedTinkerVertex implements Serializable {
    public static final String label = "song";

    public static final String NAME = "name";
    public static final String SONG_TYPE = "songType";
    public static final String PERFORMANCES = "performances";
    public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(NAME, SONG_TYPE, PERFORMANCES));

    // properties
    private String name;
    private String songType;
    private Integer performances;

    // edges
    public static final String[] ALL_EDGES = new String[] {FollowedBy.label, WrittenBy.label, SungBy.label};
    private Set<Long> followedByOut = new HashSet();
    private Set<Long> followedByIn = new HashSet();
    private Set<Long> writtenByOut = new HashSet();
    private Set<Long> sungByOut = new HashSet();

    public Song(Long id, TinkerGraph graph) {
        super(id, Song.label, graph, SPECIFIC_KEYS);
    }


    /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
    @Override
    protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
        final VertexProperty<V> ret;
        if (NAME.equals(key) && name != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, name));
        } else if (key == SONG_TYPE && songType != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, songType));
        } else if (key == PERFORMANCES && performances != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, performances));
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
        if (NAME.equals(key)) {
            this.name = (String) value;
        } else if (SONG_TYPE.equals(key)) {
            this.songType = (String) value;
        } else if (PERFORMANCES.equals(key)) {
            this.performances = (Integer) value;
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
        return property(key);
    }


    @Override
    protected void removeSpecificProperty(String key) {
        if (NAME.equals(key)) {
            this.name = null;
        } else if (SONG_TYPE.equals(key)) {
            this.songType = null;
        } else if (PERFORMANCES.equals(key)) {
            this.performances = null;
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
            if (label == FollowedBy.label) {
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    iterators.add(getFollowedByIn());
                }
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    iterators.add(getFollowedByOut());
                }
            } else if (label == WrittenBy.label) {
                if (direction == Direction.OUT|| direction == Direction.BOTH) {
                    iterators.add(getWrittenByOut());
                }
            } else if (label == SungBy.label) {
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    iterators.add(getSungByOut());
                }
            }
        }

        Iterator<Long>[] iteratorsArray = iterators.toArray(new Iterator[iterators.size()]);
        return IteratorUtils.concat(iteratorsArray);
    }

    @Override
    protected void removeSpecificOutEdge(Long edgeId) {
        followedByOut.remove(edgeId);
        writtenByOut.remove(edgeId);
        sungByOut.remove(edgeId);
    }

    @Override
    protected void removeSpecificInEdge(Long edgeId) {
        followedByIn.remove(edgeId);
    }

    @Override
    public Map<String, Set<Long>> edgeIdsByLabel(Direction direction) {
        final Map<String, Set<Long>> result = new HashMap<>();
        if (direction.equals(Direction.IN)) {
            result.put(FollowedBy.label, followedByIn);
        } else if (direction.equals(Direction.OUT)) {
            result.put(FollowedBy.label, followedByOut);
            result.put(WrittenBy.label, writtenByOut);
            result.put(SungBy.label, sungByOut);
        } else {
            throw new NotImplementedException("not implemented for direction=" + direction);
        }

        return result;
    }

    @Override
    public void addSpecializedOutEdge(String edgeLabel, long edgeId) {
        if (FollowedBy.label.equals(edgeLabel)) {
            followedByOut.add(edgeId);
        } else if (WrittenBy.label.equals(edgeLabel)) {
            writtenByOut.add(edgeId);
        } else if (SungBy.label.equals(edgeLabel)) {
            sungByOut.add(edgeId);
        } else {
            throw new IllegalArgumentException("edge type " + edgeLabel + " not supported");
        }
    }

    @Override
    public void addSpecializedInEdge(String edgeLabel, long edgeId) {
        if (FollowedBy.label.equals(edgeLabel)) {
            followedByIn.add(edgeId);
        } else {
            throw new IllegalArgumentException("edge type " + edgeLabel + " not supported");
        }
    }

    private Iterator<Long> getFollowedByOut() {
        return followedByOut.iterator();
    }

    private Iterator<Long> getFollowedByIn() {
        return followedByIn.iterator();
    }

    private Iterator<Long> getWrittenByOut() {
        return writtenByOut.iterator();
    }
    private Iterator<Long> getSungByOut() {
        return sungByOut.iterator();
    }

    public static SpecializedElementFactory.ForVertex<Song> factory = new SpecializedElementFactory.ForVertex<Song>() {
        @Override
        public String forLabel() {
            return Song.label;
        }

        @Override
        public Song createVertex(Long id, TinkerGraph graph) {
            return new Song(id, graph);
        }
    };

    public String getName() {
        return name;
    }

    public String getSongType() {
        return songType;
    }

    public Integer getPerformances() {
        return performances;
    }
}
