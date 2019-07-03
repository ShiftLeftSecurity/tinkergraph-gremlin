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
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRef;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class SerializerTestVertex extends SpecializedTinkerVertex implements Serializable {
    public static final String label = "testVertex";

    public static final String STRING_PROPERTY = "StringProperty";
    public static final String INT_PROPERTY = "IntProperty";
    public static final String STRING_LIST_PROPERTY = "StringListProperty";
    public static final String INT_LIST_PROPERTY = "IntListProperty";
    public static final String OPTIONAL_LONG_PROPERTY = "OptLongProperty";
    public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(STRING_PROPERTY, INT_PROPERTY, STRING_LIST_PROPERTY, INT_LIST_PROPERTY));
    public static final Set<String> ALLOWED_IN_EDGE_LABELS = new HashSet<>(Arrays.asList(SerializerTestEdge.label));
    public static final Set<String> ALLOWED_OUT_EDGE_LABELS = new HashSet<>(Arrays.asList(SerializerTestEdge.label));
    public static final int STRING_PROPERTY_IDX = 0;
    public static final int INT_PROPERTY_IDX = 1;
    public static final int STRING_LIST_PROPERTY_IDX = 2;
    public static final int INT_LIST_PROPERTY_IDX = 3;
    public static final int OPTIONAL_LONG_PROPERTY_IDX = 4;

    // properties
    private String stringProperty;
    private Integer intProperty;
    private List<String> stringListProperty;
    private List<Integer> intListProperty;
    private Optional<Long> optionalLongProperty;

    public SerializerTestVertex(Long id, TinkerGraph graph) {
        super(id, SerializerTestVertex.label, graph);
    }

    @Override
    protected Set<String> specificKeys() {
        return SPECIFIC_KEYS;
    }

    @Override
    public Set<String> allowedOutEdgeLabels() {
        return ALLOWED_OUT_EDGE_LABELS;
    }

    @Override
    public Set<String> allowedInEdgeLabels() {
        return ALLOWED_IN_EDGE_LABELS;
    }

    /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
    @Override
    protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
        final VertexProperty<V> ret;
        if (STRING_PROPERTY.equals(key) && stringProperty != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, stringProperty));
        } else if (key == STRING_LIST_PROPERTY && stringListProperty != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, stringListProperty));
        } else if (key == INT_PROPERTY && intProperty != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, intProperty));
        } else if (key == INT_LIST_PROPERTY && intListProperty != null) {
            return IteratorUtils.of(new TinkerVertexProperty(this, key, intListProperty));
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    public Map<String, Object> valueMap() {
        Map<String, Object> properties = new HashMap<>();
        if (stringProperty != null) properties.put(STRING_PROPERTY, stringProperty);
        if (stringListProperty != null) properties.put(STRING_LIST_PROPERTY, stringListProperty);
        if (intProperty != null) properties.put(INT_PROPERTY, intProperty);
        if (intListProperty != null) properties.put(INT_LIST_PROPERTY, intListProperty);
        if (optionalLongProperty != null) {
            optionalLongProperty.ifPresent(value -> properties.put(OPTIONAL_LONG_PROPERTY, value));
        }
        return properties;
    }

    @Override
    public SortedMap<Integer, Object> propertiesByStorageIdx() {
        SortedMap<Integer, Object> ret = new TreeMap<>();
        if (stringProperty != null) ret.put(STRING_PROPERTY_IDX, stringProperty);
        if (intProperty != null) ret.put(INT_PROPERTY_IDX, intProperty);
        if (stringListProperty != null) ret.put(STRING_LIST_PROPERTY_IDX, stringListProperty);
        if (intListProperty != null) ret.put(INT_LIST_PROPERTY_IDX, intListProperty);
        if (optionalLongProperty != null) {
            optionalLongProperty.ifPresent(value -> ret.put(OPTIONAL_LONG_PROPERTY_IDX, value));
        }
        return ret;
    }

    @Override
    protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
        if (STRING_PROPERTY.equals(key)) {
            this.stringProperty = (String) value;
        } else if (STRING_LIST_PROPERTY.equals(key)) {
            if (value instanceof List) {
                this.stringListProperty = (List) value;
            } else {
                if (this.stringListProperty == null) this.stringListProperty = new ArrayList<>();
                this.stringListProperty.add((String) value);
            }
        } else if (INT_PROPERTY.equals(key)) {
            this.intProperty = (Integer) value;
        } else if (INT_LIST_PROPERTY.equals(key)) {
            if (value instanceof List) {
                this.intListProperty = (List) value;
            } else {
                if (this.intListProperty == null) this.intListProperty = new ArrayList<>();
                this.intListProperty.add((Integer) value);
            }
        } else if (OPTIONAL_LONG_PROPERTY.equals(key)) {
            this.optionalLongProperty = (Optional) Optional.of(value);
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
        return property(key);
    }

    @Override
    protected void removeSpecificProperty(String key) {
        if (STRING_PROPERTY.equals(key)) {
            this.stringProperty = null;
        } else if (STRING_LIST_PROPERTY.equals(key)) {
            this.stringListProperty = null;
        } else if (INT_PROPERTY.equals(key)) {
            this.intProperty = null;
        } else if (INT_LIST_PROPERTY.equals(key)) {
            this.intListProperty = null;
        } else if (OPTIONAL_LONG_PROPERTY.equals(key)) {
            this.optionalLongProperty = null;
        } else {
            throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
        }
    }

    public static SpecializedElementFactory.ForVertex<SerializerTestVertex> factory = new SpecializedElementFactory.ForVertex<SerializerTestVertex>() {
        @Override
        public String forLabel() {
            return SerializerTestVertex.label;
        }

        @Override
        public SerializerTestVertex createVertex(Long id, TinkerGraph graph) {
            return new SerializerTestVertex(id, graph);
        }

        @Override
        public VertexRef<SerializerTestVertex> createVertexRef(SerializerTestVertex vertex) {
            return new VertexRef<>(vertex);
        }

        @Override
        public VertexRef<SerializerTestVertex> createVertexRef(Long id, TinkerGraph graph) {
            return new VertexRef<>(id, SerializerTestVertex.label, graph);
        }

        @Override
        public Map<Integer, Class> propertyTypeByIndex() {
            final Map<Integer, Class> ret = new THashMap<>(1);
            ret.put(STRING_PROPERTY_IDX, String.class);
            ret.put(INT_PROPERTY_IDX, Integer.class);
            ret.put(STRING_LIST_PROPERTY_IDX, String.class);
            ret.put(INT_LIST_PROPERTY_IDX, Integer.class);
            ret.put(OPTIONAL_LONG_PROPERTY_IDX, Long.class);
            return ret;
        }
    };

}
