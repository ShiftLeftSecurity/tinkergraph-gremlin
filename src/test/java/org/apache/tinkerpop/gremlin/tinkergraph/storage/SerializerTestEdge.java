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

import gnu.trove.map.hash.THashMap;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.EdgeRef;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class SerializerTestEdge extends SpecializedTinkerEdge {
    public static final String label = "testEdge";

    public static final String LONG_PROPERTY = "longProperty";
    public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(LONG_PROPERTY));
    public static final int LONG_PROPERTY_IDX = 0;

    protected Long longProperty;

    public SerializerTestEdge(TinkerGraph graph, long id, Vertex outVertex, Vertex inVertex) {
        super(graph, id, outVertex, label, inVertex, SPECIFIC_KEYS);
    }

    @Override
    public SortedMap<Integer, Object> propertiesByStorageIdx() {
        SortedMap<Integer, Object> ret = new TreeMap<>();
        if (longProperty != null) ret.put(LONG_PROPERTY_IDX, longProperty);
        return ret;
    }

    @Override
    protected <V> Property<V> specificProperty(String key) {
        final Object value;
        final boolean mandatory;
        try {
            switch (key) {
                case LONG_PROPERTY:
                    if (longProperty == null) longProperty = (Long) graph.readProperty(this, LONG_PROPERTY_IDX, Long.class);
                    value = longProperty;
                    mandatory = true;
                    break;
                default: return Property.empty();
            }

            if (mandatory) validateMandatoryProperty(key, value);
            return new TinkerProperty(this, key, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected <V> Property<V> updateSpecificProperty(String key, V value) {
        if (LONG_PROPERTY.equals(key)) {
            this.longProperty = ((Number) value).longValue();
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
        return property(key);
    }

    @Override
    protected void removeSpecificProperty(String key) {
        if (LONG_PROPERTY.equals(key)) {
            this.longProperty = null;
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
    }

    public static SpecializedElementFactory.ForEdge<SerializerTestEdge> factory = new SpecializedElementFactory.ForEdge<SerializerTestEdge>() {
        @Override
        public String forLabel() {
            return SerializerTestEdge.label;
        }

        @Override
        public SerializerTestEdge createEdge(Long id, TinkerGraph graph, VertexRef outVertex, VertexRef inVertex) {
            return new SerializerTestEdge(graph, id, outVertex, inVertex);
        }

        @Override
        public EdgeRef<SerializerTestEdge> createEdgeRef(SerializerTestEdge edge) {
            return new EdgeRef<>(edge);
        }

        @Override
        public EdgeRef<SerializerTestEdge> createEdgeRef(Long id, TinkerGraph graph, VertexRef outVertex, VertexRef inVertex) {
            return new EdgeRef<>(id, SerializerTestEdge.label, graph);
        }

        @Override
        public Map<Integer, Class> propertyTypeByIndex() {
            final Map<Integer, Class> ret = new THashMap<>(1);
            ret.put(LONG_PROPERTY_IDX, Long.class);
            return ret;
        }

        @Override
        public Map<Integer, String> propertyNamesByIndex() {
            final Map<Integer, String> ret = new THashMap<>(1);
            ret.put(LONG_PROPERTY_IDX, LONG_PROPERTY);
            return ret;
        }
    };
}
