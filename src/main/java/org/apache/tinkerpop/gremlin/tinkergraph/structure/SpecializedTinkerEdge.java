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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public abstract class SpecializedTinkerEdge<IdType> extends TinkerEdge {

    private final Set<String> specificKeys;

    protected SpecializedTinkerEdge(IdType id, Vertex outVertex, String label, Vertex inVertex, Set<String> specificKeys) {
        super(id, outVertex, label, inVertex);
        this.specificKeys = specificKeys;
    }

    @Override
    public Set<String> keys() {
        return specificKeys;
    }

    @Override
    public <V> Property<V> property(String key) {
        return specificProperty(key);
    }

    /* implement in concrete specialised instance to avoid using generic HashMaps */
    protected abstract <V> Property<V> specificProperty(String key);

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        if (propertyKeys.length == 0) {
            return (Iterator) specificKeys.stream().map(key -> property(key)).filter(vp -> vp.isPresent()).iterator();
        } else if (propertyKeys.length == 1) { // treating as special case for performance
            final Property<V> prop = property(propertyKeys[0]);
            return prop.isPresent() ? IteratorUtils.of(prop) : Collections.emptyIterator();
        } else {
            return Arrays.stream(propertyKeys).map(key -> (Property<V>) property(key)).filter(vp -> vp.isPresent()).iterator();
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw elementAlreadyRemoved(Edge.class, id);
        ElementHelper.validateProperty(key, value);
        final Property oldProperty = super.property(key);
        final Property<V> p = updateSpecificProperty(key, value);
        TinkerHelper.autoUpdateIndex(this, key, value, oldProperty.isPresent() ? oldProperty.value() : null);
        return p;
    }

    protected abstract <V> Property<V> updateSpecificProperty(String key, V value);

    @Override
    public void remove() {
        final SpecializedTinkerVertex<Long> outVertex = (SpecializedTinkerVertex<Long>) this.outVertex;
        final SpecializedTinkerVertex<Long> inVertex = (SpecializedTinkerVertex<Long>) this.inVertex;

        outVertex.removeOutEdge(this);
        inVertex.removeInEdge(this);

        TinkerHelper.removeElementIndex(this);
        ((TinkerGraph) this.graph()).edges.remove(this.id());
        this.properties = null;
        this.removed = true;
    }

}
