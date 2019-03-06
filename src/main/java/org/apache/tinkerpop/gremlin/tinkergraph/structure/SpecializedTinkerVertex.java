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

import gnu.trove.set.TLongSet;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.stream.StreamSupport;

public abstract class SpecializedTinkerVertex extends TinkerVertex {

    private final Set<String> specificKeys;

    /** `dirty` flag for serialization to avoid superfluous serialization */
    private boolean modifiedSinceLastSerialization = true;
    private Semaphore modificationSemaphore = new Semaphore(1);

    protected SpecializedTinkerVertex(long id, String label, TinkerGraph graph, Set<String> specificKeys) {
        super(id, label, graph);
        this.specificKeys = specificKeys;
    }

    @Override
    public Set<String> keys() {
        return specificKeys;
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
        if (this.removed) return VertexProperty.empty();
        return specificProperty(key);
    }

    /* You can override this default implementation in concrete specialised instances for performance
     * if you like, since technically the Iterator isn't necessary.
     * This default implementation works fine though. */
    protected <V> VertexProperty<V> specificProperty(String key) {
        Iterator<VertexProperty<V>> iter = specificProperties(key);
        if (iter.hasNext()) {
          return iter.next();
        } else {
          return VertexProperty.empty();
        }
    }

    /* implement in concrete specialised instance to avoid using generic HashMaps */
    protected abstract <V> Iterator<VertexProperty<V>> specificProperties(String key);

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        if (this.removed) return Collections.emptyIterator();
        if (propertyKeys.length == 0) { // return all properties
            return (Iterator) specificKeys.stream().flatMap(key ->
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                  specificProperties(key), Spliterator.ORDERED),false)
            ).iterator();
        } else if (propertyKeys.length == 1) { // treating as special case for performance
            return specificProperties(propertyKeys[0]);
        } else {
            return (Iterator) Arrays.stream(propertyKeys).flatMap(key ->
              StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                specificProperties(key), Spliterator.ORDERED),false)
            ).iterator();
        }
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        acquireModificationLock();
        this.modifiedSinceLastSerialization = true;
        final VertexProperty<V> vp = updateSpecificProperty(cardinality, key, value);
        TinkerHelper.autoUpdateIndex(this, key, value, null);
        releaseModificationLock();
        return vp;
    }

    protected abstract <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value);

    public void removeProperty(String key) {
        acquireModificationLock();
        modifiedSinceLastSerialization = true;
        removeSpecificProperty(key);
        releaseModificationLock();
    }

    protected abstract void removeSpecificProperty(String key);

    @Override
    public Edge addEdge(String label, Vertex vertex, Object... keyValues) {
        if (null == vertex) {
            throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        }
        if (this.removed) {
            throw elementAlreadyRemoved(Vertex.class, this.id);
        }

        if (graph.specializedEdgeFactoryByLabel.containsKey(label)) {
            SpecializedElementFactory.ForEdge factory = graph.specializedEdgeFactoryByLabel.get(label);

            Long idValue = (Long) graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
            if (null != idValue) {
                if (edgeIdAlreadyExists(idValue))
                    throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
            } else {
                idValue = (Long) graph.edgeIdManager.getNextId(graph);
            }
            graph.currentId.set(Long.max(idValue, graph.currentId.get()));

            ElementHelper.legalPropertyKeyValueArray(keyValues);
            TinkerVertex inVertex = (TinkerVertex) vertex;
            TinkerVertex outVertex = this;
            SpecializedTinkerEdge edge = factory.createEdge(idValue, graph, (long) outVertex.id, (long) inVertex.id);
            ElementHelper.attachProperties(edge, keyValues);
            if (graph.ondiskOverflowEnabled) {
                graph.getElementIdsByLabel(graph.edgeIdsByLabel, label).add(idValue);
                graph.edgeCache.put(idValue, edge);
            } else {
                graph.edges.put(idValue, edge);
            }

            acquireModificationLock();
            this.addSpecializedOutEdge(edge.label(), (Long) edge.id());
            ((SpecializedTinkerVertex) inVertex).addSpecializedInEdge(edge.label(), (Long) edge.id());
            releaseModificationLock();
            this.modifiedSinceLastSerialization = true;
            return edge;
        } else { // edge label not registered for a specialized factory, treating as generic edge
            if (graph.usesSpecializedElements) {
                throw new IllegalArgumentException(
                    "this instance of TinkerGraph uses specialized elements, but doesn't have a factory for label " + label
                        + ". Mixing specialized and generic elements is not (yet) supported");
            }
            return super.addEdge(label, vertex, keyValues);
        }
    }

    private boolean edgeIdAlreadyExists(Long idValue) {
        if (!graph.ondiskOverflowEnabled) {
            return graph.edges.containsKey(idValue);
        } else {
            for (TLongSet ids : graph.edgeIdsByLabel.values()) {
                if (ids.contains(idValue.longValue())) {
                    return true;
                }
            }
            return false;
        }
    }

    /** do not call directly (other than from deserializer)
     *  I whish there was an easy way to forbid this in java */
    public abstract void addSpecializedOutEdge(String edgeLabel, long edgeId);

    /** do not call directly (other than from deserializer)
     *  I whish there was an easy way to forbid this in java */
    public abstract void addSpecializedInEdge(String edgeLabel, long edgeId);

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return graph.edgesById(specificEdges(direction, edgeLabels));
    }

    /* implement in concrete specialised instance to avoid using generic HashMaps */
    protected abstract Iterator<Long> specificEdges(final Direction direction, final String... edgeLabels);

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        Iterator<Edge> edges = edges(direction, edgeLabels);
        if (direction == Direction.IN) {
            return IteratorUtils.map(edges, Edge::outVertex);
        } else if (direction == Direction.OUT) {
            return IteratorUtils.map(edges, Edge::inVertex);
        } else if (direction == Direction.BOTH) {
            return IteratorUtils.concat(vertices(Direction.IN, edgeLabels), vertices(Direction.OUT, edgeLabels));
        } else {
            return Collections.emptyIterator();
        }
    }

    public void removeOutEdge(Long edgeId) {
        removeSpecificOutEdge(edgeId);
    }

    protected abstract void removeSpecificOutEdge(Long edgeId);

    public void removeInEdge(Long edgeId) {
        removeSpecificInEdge(edgeId);
    }

    protected abstract void removeSpecificInEdge(Long edgeId);

    @Override
    public void remove() {
        super.remove();
        acquireModificationLock();
        Long id = (Long) this.id();

        if (graph.ondiskOverflowEnabled) {
            this.graph.vertexCache.remove(id);
            this.graph.vertexIdsByLabel.get(label()).remove(id);
            this.graph.onDiskVertexOverflow.remove(id);
        }
        this.graph.vertices.remove(id);
        edges(Direction.BOTH).forEachRemaining(Element::remove);

        this.modifiedSinceLastSerialization = true;
        releaseModificationLock();
    }

    public abstract Map<String, Set<Long>> edgeIdsByLabel(Direction direction);

    public boolean isModifiedSinceLastSerialization() {
        return modifiedSinceLastSerialization;
    }

    public void setModifiedSinceLastSerialization(boolean modifiedSinceLastSerialization) {
        this.modifiedSinceLastSerialization = modifiedSinceLastSerialization;
    }

    public void acquireModificationLock() {
        try {
            modificationSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void releaseModificationLock() {
        modificationSemaphore.release();
    }
}
