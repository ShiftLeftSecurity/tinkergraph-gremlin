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

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public abstract class SpecializedTinkerEdge extends TinkerEdge {

    private final Set<String> specificKeys;

    //using ids instead of hard references, so we can use disk storage
    public final long outVertexId;
    public final long inVertexId;

    protected SpecializedTinkerEdge(TinkerGraph graph, long id, long outVertexId, String label, long inVertexId, Set<String> specificKeys) {
        super(graph, id, null, label, null);
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
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
            // return IteratorUtils.of(property(propertyKeys[0]));
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
    public Graph graph() {
        return this.graph;
    }

    @Override
    public void remove() {
        final SpecializedTinkerVertex outVertex = (SpecializedTinkerVertex) this.outVertex;
        final SpecializedTinkerVertex inVertex = (SpecializedTinkerVertex) this.inVertex;

        outVertex.removeOutEdge(this);
        inVertex.removeInEdge(this);

        TinkerHelper.removeElementIndex(this);
        ((TinkerGraph) this.graph()).edges.remove(this.id());
        this.properties = null;
        this.removed = true;
    }

    @Override
    /** adaptation of `StringFactory.edgeString` to cover the fact that we hold the IDs rather than hard references */
    public String toString() {
        return "e[" + id() + "]" + "[" + outVertexId + "-" + label + "->" + inVertexId + "]";
    }

    @Override
    public Vertex outVertex() {
        return graph.vertexById(this.outVertexId);
    }

    @Override
    public Vertex inVertex() {
        return graph.vertexById(this.inVertexId);
    }

}
