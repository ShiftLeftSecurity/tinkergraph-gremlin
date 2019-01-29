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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import java.io.File;
import java.io.IOException;
import java.net.CacheRequest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An in-memory (with optional persistence on calls to {@link #close()}), reference implementation of the property
 * graph interfaces provided by TinkerPop.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
public final class TinkerGraph implements Graph {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(TinkerGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                TinkerGraphStepStrategy.instance(),
                TinkerGraphCountStrategy.instance()));
    }

    public static final Configuration EMPTY_CONFIGURATION() {
        return new BaseConfiguration() {{
            this.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
            this.setProperty(GREMLIN_TINKERGRAPH_GRAPH_LOCATION, "mvstore_" + System.currentTimeMillis() + ".bin");
            this.setProperty(GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "mvstore");
            this.setProperty(GREMLIN_TINKERGRAPH_VERTEX_CACHE_MAX_HEAP_PERCENTAGE, 50f);
            this.setProperty(GREMLIN_TINKERGRAPH_EDGE_CACHE_MAX_HEAP_PERCENTAGE, 30f);
        }};
    }

    public static final String GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER = "gremlin.tinkergraph.vertexIdManager";
    public static final String GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER = "gremlin.tinkergraph.edgeIdManager";
    public static final String GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER = "gremlin.tinkergraph.vertexPropertyIdManager";
    public static final String GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.tinkergraph.defaultVertexPropertyCardinality";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_LOCATION = "gremlin.tinkergraph.graphLocation";
    public static final String GREMLIN_TINKERGRAPH_GRAPH_FORMAT = "gremlin.tinkergraph.graphFormat";
    public static final String GREMLIN_TINKERGRAPH_VERTEX_CACHE_MAX_HEAP_PERCENTAGE = "gremlin.tinkergraph.vertexCache.maxHeapPercentage";
    public static final String GREMLIN_TINKERGRAPH_EDGE_CACHE_MAX_HEAP_PERCENTAGE = "gremlin.tinkergraph.edgeCache.maxHeapPercentage";

    private final TinkerGraphFeatures features = new TinkerGraphFeatures();

    protected AtomicLong currentId = new AtomicLong(-1L);
    // TODO: remove once standard tests work for specialized elements (only used for standard vertices)
    protected Map<Object, Vertex>   vertices = new ConcurrentHashMap<>();
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

    protected VertexSerializer vertexSerializer;
    protected EdgeSerializer edgeSerializer;
    protected final MVStore mvstore;
    protected final MVMap<Long, byte[]> serializedVertices;
    protected final MVMap<Long, byte[]> serializedEdges;

    /* cache for on-disk storage */
    protected final String verticesCacheName = "verticesCache";
    protected final String edgesCacheName = "edgesCache";
    protected final CacheManager cacheManager;
    protected final Cache<Long, SpecializedTinkerVertex> verticesCache;
    protected final Cache<Long, SpecializedTinkerEdge> edgesCache;


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
        mvstore = new MVStore.Builder().fileName(graphLocation).open();
        serializedVertices = mvstore.openMap("vertices");
        serializedEdges = mvstore.openMap("edges");

        if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null))
            throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
                    GREMLIN_TINKERGRAPH_GRAPH_LOCATION, GREMLIN_TINKERGRAPH_GRAPH_FORMAT));

        float maxMemory = Runtime.getRuntime().maxMemory();
        long vertexMaxHeapBytes = (long) (maxMemory / 100f * configuration.getFloat(GREMLIN_TINKERGRAPH_VERTEX_CACHE_MAX_HEAP_PERCENTAGE));
        long edgeMaxHeapBytes = (long) (maxMemory / 100f * configuration.getFloat(GREMLIN_TINKERGRAPH_EDGE_CACHE_MAX_HEAP_PERCENTAGE));
        System.out.println("initializing caches with the following max sizes: vertexCache=" + vertexMaxHeapBytes + ", edgeCache=" + edgeMaxHeapBytes);
        cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .withCache(verticesCacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, SpecializedTinkerVertex.class, ResourcePoolsBuilder.heap(vertexMaxHeapBytes)))
          .withCache(edgesCacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, SpecializedTinkerEdge.class, ResourcePoolsBuilder.heap(edgeMaxHeapBytes)))
          .build();
        cacheManager.init();
        verticesCache = cacheManager.getCache(verticesCacheName, Long.class, SpecializedTinkerVertex.class);
        edgesCache = cacheManager.getCache(edgesCacheName, Long.class, SpecializedTinkerEdge.class);

        if (graphLocation != null) loadGraph();
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

    public Edge edgeById(Long id) {
        return getElement(id, serializedEdges, edgesCache, edgeSerializer);
    }

    public Iterator<Edge> edgesById(Iterator<Long> ids) {
        Spliterator<Long> spliterator = Spliterators.spliteratorUnknownSize(ids, Spliterator.ORDERED);
        boolean parallel = false;
        Stream<Long> stream = StreamSupport.stream(spliterator, parallel);
        return stream.map(id -> edgeById(id)).iterator();
    }

    public Vertex vertexById(Long id) {
      return getElement(id, serializedVertices, verticesCache, vertexSerializer);
    }

    public Iterator<Vertex> verticesById(Iterator<Long> ids) {
        Spliterator<Long> spliterator = Spliterators.spliteratorUnknownSize(ids, Spliterator.ORDERED);
        boolean parallel = false;
        Stream<Long> stream = StreamSupport.stream(spliterator, parallel);
        return stream.map(id -> vertexById(id)).iterator();
    }



    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Long idValue = (Long) vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = (Long) vertexIdManager.getNextId(this);
        }

        if (specializedVertexFactoryByLabel.containsKey(label)) {
            SpecializedElementFactory.ForVertex factory = specializedVertexFactoryByLabel.get(label);
            SpecializedTinkerVertex vertex = factory.createVertex(idValue, this);
            ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
            try {
              this.serializedVertices.put(idValue, vertexSerializer.serialize(vertex));
            } catch (IOException e) {
              throw new RuntimeException(e);
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
        return StringFactory.graphString(this, "vertices:" + this.serializedVertices.size() + " edges:" + this.serializedEdges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.serializedVertices.clear();
        this.edges.clear();
        this.serializedEdges.clear();
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
        mvstore.close();
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
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (usesSpecializedElements) {
          return createElementIteratorFromSerialized(Vertex.class, serializedVertices, verticesCache, vertexSerializer, vertexIds);
        } else {
          return createElementIterator(Vertex.class, vertices, vertexIdManager, vertexIds);
        }
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
      if (usesSpecializedElements) {
        return createElementIteratorFromSerialized(Edge.class, serializedEdges, edgesCache, edgeSerializer, edgeIds);
      } else {
        return createElementIterator(Edge.class, edges, edgeIdManager, edgeIds);
      }
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
                } else if (graphFormat.equals("mvstore")) {
                    // TODO allow to just read back from that same file - might lead to confusion, so leaving out for now
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
            } else if (graphFormat.equals("mvstore")) {
                // TODO allow to just read back from that same file - might lead to confusion, so leaving out for now
            } else {
                io(IoCore.createIoBuilder(graphFormat)).writeGraph(graphLocation);
            }
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Could not save graph at %s with %s", graphLocation, graphFormat), ex);
        }
    }


    /** would have been nice to share the implementation with `createElementIterator` and just pass a transform Function, but that didn't work out... */
    private <T extends Element> Iterator<T> createElementIteratorFromSerialized(final Class<T> clazz,
                                                                                final Map<Long, byte[]> elements,
                                                                                final Cache<Long, ? extends T> cache,
                                                                                final Serializer<? extends T> serializer,
                                                                                final Object... ids) {
      // for whatever reason the passed objects can either be elements or their ids... :(
      final Collection<Object> idList;
      if (0 == ids.length) {
          // TODO really, is that the way to do this in java?
          idList = Arrays.asList(elements.keySet().stream().map(l -> (Object) l).toArray());
      } else {
          idList = Arrays.asList(ids);
      }

      return idList.stream().map(obj -> {
          if (clazz.isInstance(obj)) {
              return (T) obj;
          } else {
              final Long id;
              if (obj instanceof Integer) {
                  id = ((Integer) obj).longValue();
              } else {
                  id = (Long) obj;
              }
              return getElement(id, elements, cache, serializer);
          }
      }).iterator();
    }

  /** check for element in cache. deserialize and populate cache if not in cache */
    private <T extends Element> T getElement(final Long id,
                                             final Map<Long, byte[]> elements,
                                             final Cache<Long, ? extends T> cache,
                                             final Serializer<? extends T> serializer) {
      if (cache.containsKey(id)) {
        return cache.get(id);
      } else {
        try {
          T deserializedElement = serializer.deserialize(elements.get(id));
          if (deserializedElement != null) {
              ((Cache<Long, T>) cache).put(id, deserializedElement);
          }
          return deserializedElement;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz, final Map<Object, T> elements,
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
