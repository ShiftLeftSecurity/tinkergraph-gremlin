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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class OverflowDbTestEdge extends SpecializedTinkerEdge {
    public static final String label = "testEdge";

    public static final String LONG_PROPERTY = "longProperty";
    public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(LONG_PROPERTY));

    private Long longProperty;

    public OverflowDbTestEdge(TinkerGraph graph, long id, Vertex outVertex, Vertex inVertex) {
        super(graph, id, outVertex, label, inVertex, SPECIFIC_KEYS);
    }

    @Override
    protected <V> Property<V> specificProperty(String key) {
        // note: use the statically defined strings to take advantage of `==` (pointer comparison) over `.equals` (String content comparison) for performance
        if (LONG_PROPERTY.equals(key) && longProperty != null) {
            return new TinkerProperty(this, key, longProperty);
        } else {
            return Property.empty();
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

    public static SpecializedElementFactory.ForEdge<OverflowDbTestEdge> factory = new SpecializedElementFactory.ForEdge<OverflowDbTestEdge>() {
        @Override
        public String forLabel() {
            return OverflowDbTestEdge.label;
        }

        @Override
        public OverflowDbTestEdge createEdge(Long id, TinkerGraph graph, VertexRef outVertex, VertexRef inVertex) {
            return new OverflowDbTestEdge(graph, id, outVertex, inVertex);
        }

        @Override
        public EdgeRef<OverflowDbTestEdge> createEdgeRef(OverflowDbTestEdge edge) {
            return new EdgeRefWithLabel<>(edge.id(), edge.graph(), edge, OverflowDbTestEdge.label);
        }

        @Override
        public EdgeRef<OverflowDbTestEdge> createEdgeRef(Long id, TinkerGraph graph, VertexRef outVertex, VertexRef inVertex) {
            return new EdgeRefWithLabel<>(id, graph, null, OverflowDbTestEdge.label);
        }
    };
}
