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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class TinkerIndex<T extends Element> {

    protected Map<String, Map<Object, Set<Long>>> index = new ConcurrentHashMap<>();
    protected final Class<T> indexClass;
    private final Set<String> indexedKeys = new HashSet<>();
    private final TinkerGraph graph;

    public TinkerIndex(final TinkerGraph graph, final Class<T> indexClass) {
        this.graph = graph;
        this.indexClass = indexClass;
    }

    protected void put(final String key, final Object value, final long id) {
        Map<Object, Set<Long>> keyMap = this.index.get(key);
        if (null == keyMap) {
            this.index.putIfAbsent(key, new ConcurrentHashMap<>());
            keyMap = this.index.get(key);
        }
        Set<Long> ids = keyMap.get(value);
        if (null == ids) {
            keyMap.putIfAbsent(value, ConcurrentHashMap.newKeySet());
            ids = keyMap.get(value);
        }
        ids.add(id);
    }

    public List<T> get(final String key, final Object value) {
        final Map<Object, Set<Long>> keyMap = this.index.get(key);
        if (null == keyMap) {
            return Collections.emptyList();
        } else {
            Set<Long> ids = keyMap.get(value);
            if (null == ids)
                return Collections.emptyList();
            else {
                final Set<T> elements;
                if (Vertex.class.isAssignableFrom(this.indexClass)) {
                    elements = ids.stream().map(id -> (T) graph.vertexById(id)).collect(Collectors.toSet());
                } else {
                    elements = ids.stream().map(id -> (T) graph.edgeById(id)).collect(Collectors.toSet());
                }
                return new ArrayList<>(elements);
            }

        }
    }

    public long count(final String key, final Object value) {
        final Map<Object, Set<Long>> keyMap = this.index.get(key);
        if (null == keyMap) {
            return 0;
        } else {
            Set<Long> set = keyMap.get(value);
            if (null == set)
                return 0;
            else
                return set.size();
        }
    }

    public void remove(final String key, final Object value, final long id) {
        final Map<Object, Set<Long>> keyMap = this.index.get(key);
        if (null != keyMap) {
            Set<Long> ids = keyMap.get(value);
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
            for (Map<Object, Set<Long>> map : index.values()) {
                for (Set<Long> set : map.values()) {
                    set.remove(element.id());
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

        final Stream<T> elements;
        if (Vertex.class.isAssignableFrom(this.indexClass)) {
            elements = this.graph.onDiskVertexOverflow.keySet().stream().map(id -> (T) graph.vertexById(id));
        } else {
            elements = this.graph.onDiskEdgeOverflow.keySet().stream().map(id -> (T) graph.edgeById(id));
        }
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
