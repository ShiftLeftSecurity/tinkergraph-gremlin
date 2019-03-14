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
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputerView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphCountStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphStepStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.EdgeSerializer;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.Serializer;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.VertexSerializer;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.org.apache.tinkerpop.gremlin.util.iterator.ArrayBackedTLongIterator;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.org.apache.tinkerpop.gremlin.util.iterator.TLongMultiIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A fork of the in-memory reference implementation TinkerGraph featuring:
 * - using ~70% less memory (depending on your domain)
 * - strict schema enforcement (optional)
 * - on-disk overflow, i.e. elements are serialized to disk if (and only if) they don't fit into memory (optional)
 *
 * TODO MP: remove complexity in implementation by removing option to use standard elements
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public final class TinkerGraph implements Graph {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(TinkerGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                TinkerGraphStepStrategy.instance(),
                TinkerGraphCountStrategy.instance()));
    }

    public static final Configuration EMPTY_CONFIGURATION() {
        return new BaseConfiguration() {{
            this.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
            this.setProperty(GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_CACHE_MAX_HEAP_PERCENTAGE, 30f);
        }};
    }

    public static final String GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER = "gremlin.tinkergraph.vertexIdManager";
    public static final String GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER = "gremlin.tinkergraph.edgeIdManager";
    public static final String GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER = "gremlin.tinkergraph.vertexPropertyIdManager";
    public static final String GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.tinkergraph.defaultVertexPropertyCardinality";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_LOCATION = "gremlin.tinkergraph.graphLocation";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_FORMAT = "gremlin.tinkergraph.graphFormat";
    public static final String GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_ENABLED = "gremlin.tinkergraph.ondiskOverflow.enabled";
    public static final String GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_CACHE_MAX_HEAP_PERCENTAGE = "gremlin.tinkergraph.ondiskOverflow.cacheMaxHeapPercentage";
    public static final String GREMLIN_TINKERGRAPH_ONDISK_ROOT_DIR = "gremlin.tinkergraph.ondiskOverflow.rootDir";


    private final TinkerGraphFeatures features = new TinkerGraphFeatures();

    protected AtomicLong currentId = new AtomicLong(-1L);
    protected Map<Object, Vertex> vertices = new ConcurrentHashMap<>();
    protected Map<Object, Edge> edges = new ConcurrentHashMap<>();

    protected TinkerGraphVariables variables = null;
    protected TinkerGraphComputerView graphComputerView = null;
    protected TinkerIndex<TinkerVertex> vertexIndex = null;
    protected TinkerIndex<TinkerEdge> edgeIndex = null;

    protected final IdManager<?> vertexIdManager;
    protected final IdManager<?> edgeIdManager;
    protected final IdManager<?> vertexPropertyIdManager;
    protected final VertexProperty.Cardinality defaultVertexPropertyCardinality;

    protected final boolean usesSpecializedElements;
    protected final Map<String, SpecializedElementFactory.ForVertex> specializedVertexFactoryByLabel = new HashMap();
    protected final Map<String, SpecializedElementFactory.ForEdge> specializedEdgeFactoryByLabel = new HashMap();

    private final Configuration configuration;
    private final String graphLocation;
    private final String graphFormat;

    /* overflow to disk: elements are serialized on eviction from on-heap cache - off by default */
    public final boolean ondiskOverflowEnabled;
    protected THashMap<String, TLongSet> vertexIdsByLabel;
    protected THashMap<String, TLongSet> edgeIdsByLabel;
    protected CacheManager cacheManager;
    protected Cache<Long, SpecializedTinkerVertex> vertexCache;
    protected Cache<Long, SpecializedTinkerEdge> edgeCache;
    protected VertexSerializer vertexSerializer;
    protected EdgeSerializer edgeSerializer;
    private MVStore mvstoreVertices;
    private MVStore mvstoreEdges;
    protected MVMap<Long, byte[]> onDiskVertexOverflow;
    protected MVMap<Long, byte[]> onDiskEdgeOverflow;

    /**
     * An empty private constructor that initializes {@link TinkerGraph}.
     */
    private TinkerGraph(final Configuration configuration, boolean usesSpecializedElements) {
        this.configuration = configuration;
        this.usesSpecializedElements = usesSpecializedElements;
        vertexIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, Vertex.class);
        edgeIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, Edge.class);
        vertexPropertyIdManager = selectIdManager(configuration, GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, VertexProperty.class);
        defaultVertexPropertyCardinality = VertexProperty.Cardinality.valueOf(
          configuration.getString(GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.single.name()));

        graphLocation = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null);
        graphFormat = configuration.getString(GREMLIN_TINKERGRAPH_GRAPH_FORMAT, null);
        if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null))
            throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
              GREMLIN_TINKERGRAPH_GRAPH_LOCATION, GREMLIN_TINKERGRAPH_GRAPH_FORMAT));

        ondiskOverflowEnabled = configuration.getBoolean(GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_ENABLED, false);
        if (ondiskOverflowEnabled) initializeOnDiskOverflow();

        if (graphLocation != null) loadGraph();
    }

    private void initializeOnDiskOverflow() {
        vertexIdsByLabel = new THashMap<>(100);
        edgeIdsByLabel = new THashMap<>(100);

        final File mvstoreVerticesFile;
        final File mvstoreEdgesFile;
        try {
            String ondiskOverflowRootDir = configuration.getString(GREMLIN_TINKERGRAPH_ONDISK_ROOT_DIR);
            File cacheParentDir = ondiskOverflowRootDir != null ? new File(ondiskOverflowRootDir) : null;
            mvstoreVerticesFile = File.createTempFile("mvstoreVertices", ".bin", cacheParentDir);
            mvstoreEdgesFile = File.createTempFile("mvstoreEdges", ".bin", cacheParentDir);
            mvstoreVerticesFile.deleteOnExit();
            mvstoreEdgesFile.deleteOnExit();
            System.out.println("on-disk cache overflow files: " + mvstoreVerticesFile + ", " + mvstoreVerticesFile);
        } catch (IOException e) {
            throw new RuntimeException("cannot create tmp file for mvstore", e);
        }
        mvstoreVertices = new MVStore.Builder().fileName(mvstoreVerticesFile.getAbsolutePath()).open();
        mvstoreEdges = new MVStore.Builder().fileName(mvstoreEdgesFile.getAbsolutePath()).open();
        onDiskVertexOverflow = mvstoreVertices.openMap("vertices");
        onDiskEdgeOverflow = mvstoreEdges.openMap("edges");

        // initialize cache (on-heap, overflow to disk)
        float maxMemory = Runtime.getRuntime().maxMemory();
        long cacheMaxHeapMegabytes = (long) (maxMemory / 100f * configuration.getFloat(GREMLIN_TINKERGRAPH_ONDISK_OVERFLOW_CACHE_MAX_HEAP_PERCENTAGE) / 1024f / 1024f);
        System.out.println("using " + cacheMaxHeapMegabytes + "m for element cache (anything above will be serialized to disk)");
        ResourcePools resourcePools = ResourcePoolsBuilder.newResourcePoolsBuilder().heap(cacheMaxHeapMegabytes, MemoryUnit.MB).build();

        CacheEventListener<Long, Vertex> vertexCacheEventListener = event -> {
            if (event.getType().equals(EventType.REMOVED)) {
                onDiskVertexOverflow.remove(event.getKey());
            } else if (event.getType().equals(EventType.EVICTED)) {
                final SpecializedTinkerVertex vertex = (SpecializedTinkerVertex) event.getOldValue();
                final Long id = (Long) vertex.id();
                if (!onDiskVertexOverflow.containsKey(id) || vertex.isModifiedSinceLastSerialization()) {
                    final byte[] serialized;
                    try {
                        serialized = vertexSerializer.serialize(vertex);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("unable to serialize " + vertex, e);
                    }
                    vertex.setModifiedSinceLastSerialization(false);
                    onDiskVertexOverflow.put(id, serialized);
                }

            }
        };
        CacheEventListener<Long, Edge> edgeCacheEventListener = event -> {
            if (event.getType().equals(EventType.REMOVED)) {
                onDiskEdgeOverflow.remove(event.getKey());
            } else if (event.getType().equals(EventType.EVICTED)) {
                final SpecializedTinkerEdge edge = (SpecializedTinkerEdge) event.getOldValue();
                final Long id = (Long) edge.id();
                if (!onDiskVertexOverflow.containsKey(id) || edge.isModifiedSinceLastSerialization()) {
                    final byte[] serialized;
                    try {
                        serialized = edgeSerializer.serialize(edge);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("unable to serialize " + edge, e);
                    }
                    edge.setModifiedSinceLastSerialization(false);
                    onDiskEdgeOverflow.put(id, serialized);
                }
            }
        };

        CacheEventListenerConfigurationBuilder vertexCacheEventListenerConfig = CacheEventListenerConfigurationBuilder
          .newEventListenerConfiguration(vertexCacheEventListener, EventType.EVICTED, EventType.REMOVED).synchronous().unordered();
        CacheEventListenerConfigurationBuilder edgeCacheEventListenerConfig = CacheEventListenerConfigurationBuilder
          .newEventListenerConfiguration(edgeCacheEventListener, EventType.EVICTED, EventType.REMOVED).synchronous().unordered();
        final String verticesCacheName = "vertexCache";
        final String edgesCacheName = "edgeCache";
        cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache(verticesCacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, SpecializedTinkerVertex.class, resourcePools).add(vertexCacheEventListenerConfig))
          .withCache(edgesCacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, SpecializedTinkerEdge.class, resourcePools).add(edgeCacheEventListenerConfig))
          .build();
        cacheManager.init();
        vertexCache = cacheManager.getCache(verticesCacheName, Long.class, SpecializedTinkerVertex.class);
        edgeCache = cacheManager.getCache(edgesCacheName, Long.class, SpecializedTinkerEdge.class);
    }

    /**
     * Open a new {@link TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link Graph} implementation does not require a {@code Configuration}
     * (or perhaps has a default configuration) it can choose to implement a zero argument
     * {@code open()} method. This is an optional constructor method for TinkerGraph. It is not enforced by the Gremlin
     * Test Suite.
     */
    public static TinkerGraph open() {
        return open(EMPTY_CONFIGURATION());
    }

    /**
     * Open a new {@code TinkerGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the {@link GraphFactory} to instantiate
     * {@link Graph} instances.  This method must be overridden for the Structure Test Suite to pass. Implementers have
     * latitude in terms of how exceptions are handled within this method.  Such exceptions will be considered
     * implementation specific by the test suite as all test generate graph instances by way of
     * {@link GraphFactory}. As such, the exceptions get generalized behind that facade and since
     * {@link GraphFactory} is the preferred method to opening graphs it will be consistent at that level.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link Graph}
     */
    public static TinkerGraph open(final Configuration configuration) {
        return new TinkerGraph(configuration, false);
    }


    public static TinkerGraph open(List<SpecializedElementFactory.ForVertex<?>> vertexFactories,
                                   List<SpecializedElementFactory.ForEdge<?>> edgeFactories) {
        return open(EMPTY_CONFIGURATION(), vertexFactories, edgeFactories);
    }

    public static TinkerGraph open(final Configuration configuration,
                                   List<SpecializedElementFactory.ForVertex<?>> vertexFactories,
                                   List<SpecializedElementFactory.ForEdge<?>> edgeFactories) {
        boolean usesSpecializedElements = !vertexFactories.isEmpty() || !edgeFactories.isEmpty();
        TinkerGraph tg =  new TinkerGraph(configuration, usesSpecializedElements);
        vertexFactories.forEach(factory -> tg.specializedVertexFactoryByLabel.put(factory.forLabel(), factory));
        edgeFactories.forEach(factory -> tg.specializedEdgeFactoryByLabel.put(factory.forLabel(), factory));
        tg.vertexSerializer = new VertexSerializer(tg, tg.specializedVertexFactoryByLabel);
        tg.edgeSerializer = new EdgeSerializer(tg, tg.specializedEdgeFactoryByLabel);
        return tg;
    }

    public Edge edgeById(long id) {
        if (ondiskOverflowEnabled)
            return getElementFromCache(id, edgeCache, onDiskEdgeOverflow, edgeSerializer);
        else
            return edges.get(id);
    }

    public Iterator<Edge> edgesById(TLongIterator ids) {
        if (ondiskOverflowEnabled) {
            return createElementIteratorForCached(edgeCache, onDiskEdgeOverflow, edgeSerializer, ids);
        } else {
            return new Iterator<Edge>() {
                @Override
                public boolean hasNext() {
                    return ids.hasNext();
                }
                @Override
                public Edge next() {
                    return edgeById(ids.next());
                }
            };
        }
    }

    public Vertex vertexById(long id) {
        if (ondiskOverflowEnabled)
            return getElementFromCache(id, vertexCache, onDiskVertexOverflow, vertexSerializer);
        else
            return vertices.get(id);
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Long idValue = (Long) vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (vertexIdAlreadyExists(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = (Long) vertexIdManager.getNextId(this);
        }
        currentId.set(Long.max(idValue, currentId.get()));

        if (specializedVertexFactoryByLabel.containsKey(label)) {
            SpecializedElementFactory.ForVertex factory = specializedVertexFactoryByLabel.get(label);
            SpecializedTinkerVertex vertex = factory.createVertex(idValue, this);
            ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
            if (ondiskOverflowEnabled) {
                getElementIdsByLabel(vertexIdsByLabel, label).add(idValue);
                vertexCache.put(idValue, vertex);
            } else {
                vertices.put(idValue, vertex);
            }
            return vertex;
        } else { // vertex label not registered for a specialized factory, treating as generic vertex
            if (this.usesSpecializedElements) {
                throw new IllegalArgumentException(
                    "this instance of TinkerGraph uses specialized elements, but doesn't have a factory for label " + label
                    + ". Mixing specialized and generic elements is not (yet) supported");
            }
            final Vertex vertex = new TinkerVertex(idValue, label, this);
            this.vertices.put(vertex.id(), vertex);
            ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
            return vertex;
        }
    }

    private boolean vertexIdAlreadyExists(Long idValue) {
        if (!ondiskOverflowEnabled) {
            return vertices.containsKey(idValue);
        } else {
            for (TLongSet ids : vertexIdsByLabel.values()) {
                if (ids.contains(idValue.longValue())) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        if (!graphComputerClass.equals(TinkerGraphComputer.class))
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
        return (C) new TinkerGraphComputer(this);
    }

    @Override
    public GraphComputer compute() {
        return new TinkerGraphComputer(this);
    }

    @Override
    public Variables variables() {
        if (null == this.variables)
            this.variables = new TinkerGraphVariables();
        return this.variables;
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        if (builder.requiresVersion(GryoVersion.V1_0) || builder.requiresVersion(GraphSONVersion.V1_0))
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV1d0.instance())).create();
        else if (builder.requiresVersion(GraphSONVersion.V2_0))   // there is no gryo v2
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV2d0.instance())).create();
        else
            return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(TinkerIoRegistryV3d0.instance())).create();
    }

    @Override
    public String toString() {
        final int vertexCount;
        final int edgeCount;
        if (usesSpecializedElements && ondiskOverflowEnabled) {
            int vSum = 0;
            int eSum = 0;
            for (TLongSet ids : vertexIdsByLabel.values())
                vSum += ids.size();
            for (TLongSet ids : edgeIdsByLabel.values())
                eSum += ids.size();
            vertexCount = vSum;
            edgeCount = eSum;
        } else {
            vertexCount = vertices.size();
            edgeCount = edges.size();
        }
        return StringFactory.graphString(this, "vertices: " + vertexCount + ", edges: " + edgeCount);
    }

    public void clear() {
        this.vertices.clear();
        this.vertexIdsByLabel.clear();
        this.edges.clear();
        this.edgeIdsByLabel.clear();
        this.onDiskVertexOverflow.clear();
        this.onDiskEdgeOverflow.clear();
        this.variables = null;
        this.currentId.set(-1L);
        this.vertexIndex = null;
        this.edgeIndex = null;
        this.graphComputerView = null;
    }

    /**
     * This method only has an effect if the {@link #GREMLIN_TINKERGRAPH_GRAPH_LOCATION} is set, in which case the
     * data in the graph is persisted to that location. This method may be called multiple times and does not release
     * resources.
     */
    @Override
    public void close() {
        if (graphLocation != null) saveGraph();
        if (ondiskOverflowEnabled) {
            mvstoreVertices.close();
            mvstoreEdges.close();
        }
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... ids) {
        if (usesSpecializedElements && ondiskOverflowEnabled) {
          return createElementIteratorForCached(vertexCache, onDiskVertexOverflow, vertexSerializer, idsIterator(vertexIdsByLabel, ids));
        } else {
          return createElementIterator(Vertex.class, vertices, vertexIdManager, ids);
        }
    }

    public Iterator<Vertex> verticesByLabel(final P<String> labelPredicate) {
        if (usesSpecializedElements && ondiskOverflowEnabled) {
            TLongIterator idsIterator = elementIdsByLabel(vertexIdsByLabel, labelPredicate);
            return createElementIteratorForCached(vertexCache, onDiskVertexOverflow, vertexSerializer, idsIterator);
        } else {
            throw new NotImplementedException("verticesWithLabel only implemented for specialized elements with ondisk overflow");
        }
    }

    @Override
    public Iterator<Edge> edges(final Object... ids) {
      if (usesSpecializedElements && ondiskOverflowEnabled) {
          return createElementIteratorForCached(edgeCache, onDiskEdgeOverflow, edgeSerializer, idsIterator(edgeIdsByLabel, ids));
      } else {
        return createElementIterator(Edge.class, edges, edgeIdManager, ids);
      }
    }

    public Iterator<Edge> edgesByLabel(final P<String> labelPredicate) {
        if (usesSpecializedElements && ondiskOverflowEnabled) {
            TLongIterator idsIterator = elementIdsByLabel(edgeIdsByLabel, labelPredicate);
            return createElementIteratorForCached(edgeCache, onDiskEdgeOverflow, edgeSerializer, idsIterator);
        } else {
            throw new NotImplementedException("edgesWithLabel only implemented for specialized elements with ondisk overflow");
        }
    }

    protected TLongSet getElementIdsByLabel(final THashMap<String, TLongSet> elementIdsByLabel, final String label) {
        if (!elementIdsByLabel.containsKey(label))
            elementIdsByLabel.put(label, new TLongHashSet(100000));
        return elementIdsByLabel.get(label);
    }

    protected TLongIterator elementIdsByLabel(final THashMap<String, TLongSet> elementIdsByLabel, final P<String> labelPredicate) {
        List<TLongIterator> iterators = new ArrayList(elementIdsByLabel.size());
        for (String label : elementIdsByLabel.keySet()) {
            if (labelPredicate.test(label)) {
                iterators.add(elementIdsByLabel.get(label).iterator());
            }
        }
        return new TLongMultiIterator(iterators);
    }

    protected TLongIterator idsIterator(THashMap<String, TLongSet> elementIdsByLabel, Object... ids) {
        final TLongIterator idsIterator;

        if (ids.length == 0) {
            // warning: this may be problematic in conjunction with concurrent modification... test
            List<TLongIterator> iterators = new ArrayList(elementIdsByLabel.size());
            for (TLongSet set : elementIdsByLabel.values()) {
                iterators.add(set.iterator());
            }
            idsIterator = new TLongMultiIterator(iterators);
        } else {
            // unfortunately because the TP api allows for any type of id (and even elements instead of ids) we have to copy the whole array...
            // unfortunately `arraycopy` fails if `ids` contains Integers, so gotta go the slow way
//                Long[] longIds = new Long[ids.length];
//                System.arraycopy(ids, 0, longIds, 0, ids.length);
            long[] longIds = new long[ids.length];
            for (int i = 0; i < ids.length; i++) {
                Object id = ids[i];
                final long longId;
                if (id instanceof Long) {
                    longId = ((Long) id).longValue();
                } else if (id instanceof Integer) {
                    longId = ((Integer) id).longValue();
                } else {
                    throw new AssertionError("provided ID=" + id + " must be a long (or integer) value, but is a " + id.getClass());
                }
                longIds[i] = longId;
            }

            idsIterator = new ArrayBackedTLongIterator(longIds);
        }
        return idsIterator;
    }

    private void loadGraph() {
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) {
            try {
                if (graphFormat.equals("graphml")) {
                    io(IoCore.graphml()).readGraph(graphLocation);
                } else if (graphFormat.equals("graphson")) {
                    io(IoCore.graphson()).readGraph(graphLocation);
                } else if (graphFormat.equals("gryo")) {
                    io(IoCore.gryo()).readGraph(graphLocation);
                } else {
                    io(IoCore.createIoBuilder(graphFormat)).readGraph(graphLocation);
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Could not load graph at %s with %s", graphLocation, graphFormat), ex);
            }
        }
    }

    private void saveGraph() {
        final File f = new File(graphLocation);
        if (f.exists()) {
            f.delete();
        } else {
            final File parent = f.getParentFile();

            // the parent would be null in the case of an relative path if the graphLocation was simply: "f.gryo"
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
        }

        try {
            if (graphFormat.equals("graphml")) {
                io(IoCore.graphml()).writeGraph(graphLocation);
            } else if (graphFormat.equals("graphson")) {
                io(IoCore.graphson()).writeGraph(graphLocation);
            } else if (graphFormat.equals("gryo")) {
                io(IoCore.gryo()).writeGraph(graphLocation);
            } else {
                io(IoCore.createIoBuilder(graphFormat)).writeGraph(graphLocation);
            }
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Could not save graph at %s with %s", graphLocation, graphFormat), ex);
        }
    }


    private <T extends Element> Iterator<T> createElementIteratorForCached(final Cache<Long, ? extends T> cache,
                                                                           final MVMap<Long, byte[]> onDiskElementOverflow,
                                                                           final Serializer<? extends T> serializer,
                                                                           final TLongIterator idsIterator) {
          return new Iterator<T>() {
              @Override
              public boolean hasNext() {
                  return idsIterator.hasNext();
              }
              @Override
              public T next() {
                  long id = idsIterator.next();
                  return getElementFromCache(id, cache, onDiskElementOverflow, serializer);
              }
          };
    }


    /** check for element in cache, otherwise read from `onDiskOverflow`, deserialize and put back in cache */
    private <T extends Element> T getElementFromCache(final Long id,
                                                      final Cache<Long, ? extends T> cache,
                                                      final MVMap<Long, byte[]> onDiskElementOverflow,
                                                      final Serializer<? extends T> serializer) {
      if (cache.containsKey(id)) {
          return cache.get(id);
      } else {
          try {
              T deserializedElement = serializer.deserialize(onDiskElementOverflow.get(id));
              if (deserializedElement != null) {
                  ((Cache<Long, T>) cache).put(id, deserializedElement);
              }
              return deserializedElement;
          } catch (IOException e) {
              throw new RuntimeException(e);
          }
      }
    }

    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz,
                                                                  final Map<Object, T> elements,
                                                                  final IdManager idManager,
                                                                  final Object... ids) {
        final Iterator<T> iterator;
        if (0 == ids.length) {
            iterator = elements.values().iterator();
        } else {
            final List<Object> idList = Arrays.asList(ids);
            validateHomogenousIds(idList);

            // if the type is of Element - have to look each up because it might be an Attachable instance or
            // other implementation. the assumption is that id conversion is not required for detached
            // stuff - doesn't seem likely someone would detach a Titan vertex then try to expect that
            // vertex to be findable in OrientDB
            return clazz.isAssignableFrom(ids[0].getClass()) ?
                    IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(clazz.cast(id).id())).iterator(), Objects::nonNull)
                    : IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(idManager.convert(id))).iterator(), Objects::nonNull);
        }
        return TinkerHelper.inComputerMode(this) ?
                (Iterator<T>) (clazz.equals(Vertex.class) ?
                        IteratorUtils.filter((Iterator<Vertex>) iterator, t -> this.graphComputerView.legalVertex(t)) :
                        IteratorUtils.filter((Iterator<Edge>) iterator, t -> this.graphComputerView.legalEdge(t.outVertex(), t))) :
                iterator;
    }

    /**
     * Return TinkerGraph feature set.
     * <p/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all
     * {@link org.apache.tinkerpop.gremlin.structure.Graph.Features} return true.
     */
    @Override
    public Features features() {
        return features;
    }

    private void validateHomogenousIds(final List<Object> ids) {
        final Iterator<Object> iterator = ids.iterator();
        Object id = iterator.next();
        if (id == null)
            throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        final Class firstClass = id.getClass();
        while (iterator.hasNext()) {
            id = iterator.next();
            if (id == null || !id.getClass().equals(firstClass))
                throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        }
    }

    public class TinkerGraphFeatures implements Features {

        private final TinkerGraphGraphFeatures graphFeatures = new TinkerGraphGraphFeatures();
        private final TinkerGraphEdgeFeatures edgeFeatures = new TinkerGraphEdgeFeatures();
        private final TinkerGraphVertexFeatures vertexFeatures = new TinkerGraphVertexFeatures();

        private TinkerGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

    }

    public class TinkerGraphVertexFeatures implements Features.VertexFeatures {

        private final TinkerGraphVertexPropertyFeatures vertexPropertyFeatures = new TinkerGraphVertexPropertyFeatures();

        private TinkerGraphVertexFeatures() {
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return defaultVertexPropertyCardinality;
        }
    }

    public class TinkerGraphEdgeFeatures implements Features.EdgeFeatures {

        private TinkerGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return edgeIdManager.allow(id);
        }
    }

    public class TinkerGraphGraphFeatures implements Features.GraphFeatures {

        private TinkerGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

    }

    public class TinkerGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        private TinkerGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null == this.vertexIndex) this.vertexIndex = new TinkerIndex<>(this, TinkerVertex.class);
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null == this.edgeIndex) this.edgeIndex = new TinkerIndex<>(this, TinkerEdge.class);
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
     *
     * @param key          the property key to stop indexing
     * @param elementClass the element class of the index to drop
     * @param <E>          The type of the element class
     */
    public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null != this.vertexIndex) this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null != this.edgeIndex) this.edgeIndex.dropKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return null == this.vertexIndex ? Collections.emptySet() : this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return null == this.edgeIndex ? Collections.emptySet() : this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Construct an {@link TinkerGraph.IdManager} from the TinkerGraph {@code Configuration}.
     */
    private static IdManager<?> selectIdManager(final Configuration config, final String configKey, final Class<? extends Element> clazz) {
        return DefaultIdManager.LONG;
    }

    /**
     * TinkerGraph will use an implementation of this interface to generate identifiers when a user does not supply
     * them and to handle identifier conversions when querying to provide better flexibility with respect to
     * handling different data types that mean the same thing.  For example, the
     * {@link DefaultIdManager#LONG} implementation will allow {@code g.vertices(1l, 2l)} and
     * {@code g.vertices(1, 2)} to both return values.
     *
     * @param <T> the id type
     */
    public interface IdManager<T> {
        /**
         * Generate an identifier which should be unique to the {@link TinkerGraph} instance.
         */
        T getNextId(final TinkerGraph graph);

        /**
         * Convert an identifier to the type required by the manager.
         */
        T convert(final Object id);

        /**
         * Determine if an identifier is allowed by this manager given its type.
         */
        boolean allow(final Object id);
    }

    /**
     * A default set of {@link IdManager} implementations for common identifier types.
     */
    public enum DefaultIdManager implements IdManager {
        /**
         * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
         * {@link Long} and will also attempt to convert {@code String} values
         */
        LONG {
            @Override
            public Long getNextId(final TinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Long)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).longValue();
                else if (id instanceof String)
                    return Long.parseLong((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        /**
         * Manages identifiers of type {@code Integer}. Will convert any class that extends from {@link Number} to a
         * {@link Integer} and will also attempt to convert {@code String} values
         */
        INTEGER {
            @Override
            public Integer getNextId(final TinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).map(Long::intValue).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Integer)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).intValue();
                else if (id instanceof String)
                    return Integer.parseInt((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Integer but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        /**
         * Manages identifiers of type {@link java.util.UUID}. Will convert {@code String} values to
         * {@link java.util.UUID}.
         */
        UUID {
            @Override
            public UUID getNextId(final TinkerGraph graph) {
                return java.util.UUID.randomUUID();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof java.util.UUID)
                    return id;
                else if (id instanceof String)
                    return java.util.UUID.fromString((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to UUID but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof UUID || id instanceof String;
            }
        },

        /**
         * Manages identifiers of any type.  This represents the default way {@link TinkerGraph} has always worked.
         * In other words, there is no identifier conversion so if the identifier of a vertex is a {@code Long}, then
         * trying to request it with an {@code Integer} will have no effect. Also, like the original
         * {@link TinkerGraph}, it will generate {@link Long} values for identifiers.
         */
        ANY {
            @Override
            public Long getNextId(final TinkerGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                return id;
            }

            @Override
            public boolean allow(final Object id) {
                return true;
            }
        }
    }
}
