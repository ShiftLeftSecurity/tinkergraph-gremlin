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

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TinkerIndex<T extends Element> {

    protected Map<String, Map<Object, TLongSet>> index = new ConcurrentHashMap<>();
    protected final Class<T> indexClass;
    private final Set<String> indexedKeys = new HashSet<>();
    private final TinkerGraph graph;

    public TinkerIndex(final TinkerGraph graph, final Class<T> indexClass) {
        this.graph = graph;
        this.indexClass = indexClass;
    }

    protected void put(final String key, final Object value, final long id) {
        Map<Object, TLongSet> keyMap = this.index.get(key);
        if (null == keyMap) {
            this.index.putIfAbsent(key, new ConcurrentHashMap<>());
            keyMap = this.index.get(key);
        }
        TLongSet ids = keyMap.get(value);
        if (null == ids) {
            keyMap.putIfAbsent(value, new TLongHashSet());
            ids = keyMap.get(value);
        }
        ids.add(id);
    }

    public List<T> get(final String key, final Object value) {
        final Map<Object, TLongSet> keyMap = this.index.get(key);
        if (null == keyMap) {
            return Collections.emptyList();
        } else {
            TLongSet ids = keyMap.get(value);
            if (null == ids)
                return Collections.emptyList();
            else {
                TLongIterator idsIter = ids.iterator();
                final List<T> elements = new ArrayList<>(ids.size());
                if (Vertex.class.isAssignableFrom(this.indexClass)) {
                    while (idsIter.hasNext()) {
                        elements.add((T) graph.vertexById(idsIter.next()));
                    }
                } else {
                    while (idsIter.hasNext()) {
                        elements.add((T) graph.edgeById(idsIter.next()));
                    }
                }
                return elements;
            }

        }
    }

    public long count(final String key, final Object value) {
        final Map<Object, TLongSet> keyMap = this.index.get(key);
        if (null == keyMap) {
            return 0;
        } else {
            TLongSet set = keyMap.get(value);
            if (null == set)
                return 0;
            else
                return set.size();
        }
    }

    public void remove(final String key, final Object value, final long id) {
        final Map<Object, TLongSet> keyMap = this.index.get(key);
        if (null != keyMap) {
            TLongSet ids = keyMap.get(value);
            if (null != ids) {
                ids.remove(id);
                if (ids.size() == 0) {
                    keyMap.remove(value);
                }
            }
        }
    }

    public void removeElement(final T element) {
        if (this.indexClass.isAssignableFrom(element.getClass())) {
            for (Map<Object, TLongSet> map : index.values()) {
                for (TLongSet set : map.values()) {
                    set.remove((Long) element.id());
                }
            }
        }
    }

    public void autoUpdate(final String key, final Object newValue, final Object oldValue, final long id) {
        if (this.indexedKeys.contains(key)) {
            if (oldValue != null)
                this.remove(key, oldValue, id);
            this.put(key, newValue, id);
        }
    }

    public void autoRemove(final String key, final Object oldValue, final long id) {
        if (this.indexedKeys.contains(key))
            this.remove(key, oldValue, id);
    }

    public void createKeyIndex(final String key) {
        if (null == key)
            throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (key.isEmpty())
            throw new IllegalArgumentException("The key for the index cannot be an empty string");

        if (this.indexedKeys.contains(key))
            return;
        this.indexedKeys.add(key);


        final Iterator<T> elementsIter;
        if (Vertex.class.isAssignableFrom(this.indexClass)) {
             elementsIter = (Iterator<T>) graph.vertices();
        } else {
            elementsIter = (Iterator<T>) graph.edges();
        }
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(elementsIter, Spliterator.ORDERED);
        boolean parallel = false;
        final Stream<T> elements = StreamSupport.stream(spliterator, parallel);
        elements.map(e -> new Object[]{((T) e).property(key), e.id()})
                .filter(a -> ((Property) a[0]).isPresent())
                .forEach(a -> this.put(key, ((Property) a[0]).value(), (Long) a[1]));
    }

    public void dropKeyIndex(final String key) {
        if (this.index.containsKey(key))
            this.index.remove(key).clear();

        this.indexedKeys.remove(key);
    }

    public Set<String> getIndexedKeys() {
        return this.indexedKeys;
    }
}
