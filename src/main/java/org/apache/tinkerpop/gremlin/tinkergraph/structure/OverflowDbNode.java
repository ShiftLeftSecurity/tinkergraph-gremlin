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

import gnu.trove.set.hash.THashSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.storage.iterator.MultiIterator2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Variant of SpecializedTinkerVertex that stores adjacent Nodes directly, rather than via edges.
 * Motivation: in many graph use cases, edges don't hold any properties and thus accounts for more memory and
 * traversal time than necessary
 *
 * TODO: extend Vertex (rather than SpecializedTinkerVertex) to save more memory
 */
public abstract class OverflowDbNode extends SpecializedTinkerVertex {

  // TODO preallocate some length, grow when needed (by more than what's immediately required to leave some space)
  private Object[] adjacentVerticesWithProperties = new Object[0];

  /* store the start offset and length into the above `adjacentVerticesWithProperties` array in an interleaved manner,
   * i.e. each outgoing edge type has two entries in this array. */
  private final int[] edgeOffsets;

  /**
   * @param numberOfDifferentAdjacentTypes The number fo different IN|OUT edge relations. E.g. a node has AST edges in
   *                                       and out, then we would have 2. If in addition it has incoming
   *                                       ref edges it would have 3.
   */
  protected OverflowDbNode(long id,
                           TinkerGraph graph,
                           int numberOfDifferentAdjacentTypes) {
    super(id, graph);
    edgeOffsets = new int[numberOfDifferentAdjacentTypes * 2];
  }

  /**
   * @return The position in edgeOffsets array. -1 if the edge label is not supported
   */
  protected abstract int getPositionInEdgeOffsets(Direction direction, String label);

  protected abstract int getOffsetRelativeToAdjacentVertexRef(String edgeLabelAndKey);

  protected abstract int getEdgeKeyCount(String edgeLabel);

  public <V> Iterator<Property<V>> getEdgeProperty(String edgeLabel,
                                                   VertexRef<OverflowDbNode> inVertex,
                                                   String... keys) {
    List<Property<V>> result = new ArrayList<>();
    for (String key: keys) {
      result.add(getEdgeProperty(edgeLabel, key, inVertex));
    }

    return result.iterator();
  }

  private <V> Property<V> getEdgeProperty(String edgeLabel,
                                          String key,
                                          VertexRef<OverflowDbNode> inVertex) {
    int propertyPosition = getPropertyIndex(edgeLabel, key, inVertex);
    V value = (V) adjacentVerticesWithProperties[propertyPosition];
    VertexRef<OverflowDbNode> thisVertexRef = (VertexRef) graph.vertex((Long) id());
    return new OverflowProperty<>(key, value, instantiateDummyEdge(edgeLabel, thisVertexRef, inVertex));
  }

  public <V> Property<V> setEdgeProperty(String edgeLabel,
                                         String key,
                                         V value,
                                         VertexRef<OverflowDbNode> inVertex,
                                         OverflowDbEdge edge) {
    int propertyPosition = getPropertyIndex(edgeLabel, key, inVertex);
    adjacentVerticesWithProperties[propertyPosition] = value;
    return new OverflowProperty<>(key, value, edge);
  }

  private int getPropertyIndex(String label, String key, VertexRef<OverflowDbNode> inVertex) {
    int offsetPos = getPositionInEdgeOffsets(Direction.OUT, label);
    int start = startIndex(offsetPos);
    int length = blockLength(offsetPos);
    int strideSize = getEdgeKeyCount(label) + 1;
    int propertyOffset = getOffsetRelativeToAdjacentVertexRef(label + key);

    for (int i = start; i < start + length; i += strideSize) {
      if (((VertexRef)adjacentVerticesWithProperties[i]).id() == inVertex.id()) {
        int vertexOutRefIndex = i - start;
        return vertexOutRefIndex + propertyOffset;
      }
    }
    return -1;
  }

  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    final VertexRef<OverflowDbNode> inVertexRef;
    if (inVertex instanceof VertexRef) inVertexRef = (VertexRef<OverflowDbNode>) inVertex;
    else inVertexRef = (VertexRef<OverflowDbNode>) graph.vertex((Long) inVertex.id());
    OverflowDbNode inVertexOdb = inVertexRef.get();
    VertexRef<OverflowDbNode> thisVertexRef = (VertexRef) graph.vertex((Long) id());

    storeAdjacentOutNode(label, inVertexRef, toMap(keyValues));
    inVertexOdb.storeAdjacentInNode(label, thisVertexRef);

    OverflowDbEdge dummyEdge = instantiateDummyEdge(label, thisVertexRef, inVertexRef);
    return dummyEdge;
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    final Labels labels = calculateInOutLabelsToFollow(direction, edgeLabels);
    final MultiIterator2<Edge> multiIterator = new MultiIterator2<>();
    VertexRef thisRef = (VertexRef)graph.vertex(id);
    for (String label : labels.forInEdges) {
      Iterator<VertexRef> vertexRefIterator = createAdjacentVertexRefIterator(Direction.IN, label);
      List<Edge> edgeList = new ArrayList<>();
      vertexRefIterator.forEachRemaining( vertexRef -> {
        edgeList.add(instantiateDummyEdge(label, vertexRef, thisRef));
      });
      multiIterator.addIterator(edgeList.iterator());
    }
    for (String label : labels.forOutEdges) {
      Iterator<VertexRef> vertexRefIterator = createAdjacentVertexRefIterator(Direction.OUT, label);
      List<Edge> edgeList = new ArrayList<>();
      vertexRefIterator.forEachRemaining( vertexRef -> {
        edgeList.add(instantiateDummyEdge(label, thisRef, vertexRef));
      });
      multiIterator.addIterator(edgeList.iterator());
    }

    return multiIterator;
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    final Labels labels = calculateInOutLabelsToFollow(direction, edgeLabels);
    final MultiIterator2<Vertex> multiIterator = new MultiIterator2<>();
    for (String label : labels.forInEdges) {
      multiIterator.addIterator(createAdjacentVertexRefIterator(Direction.IN, label));
    }
    for (String label : labels.forOutEdges) {
      multiIterator.addIterator(createAdjacentVertexRefIterator(Direction.OUT, label));
    }

    return multiIterator;
  }

  private Iterator<VertexRef> createAdjacentVertexRefIterator(Direction direction, String label) {
    int offsetPos = getPositionInEdgeOffsets(direction, label);
    if (offsetPos != -1) {
      int start = startIndex(offsetPos);
      int length = blockLength(offsetPos);
      int strideSize = getEdgeKeyCount(label) + 1;

      return new ArrayOffsetIterator<>(adjacentVerticesWithProperties, start, start + length, strideSize);
    } else {
      return Collections.emptyIterator();
    }
  }

  private static class ArrayOffsetIterator<T> implements Iterator<T>  {
    private final Object[] array;
    private int current;
    private final int end;
    private final int strideSize;

    ArrayOffsetIterator(Object[] array, int begin, int end, int strideSize) {
      this.array = array;
      this.current = begin;
      this.end = end;
      this.strideSize = strideSize;
    }

    @Override
    public boolean hasNext() {
      return current < end;
    }

    @Override
    public T next() {
      T element = (T) array[current];
      current += strideSize;
      return element;
    }
  }

  private void storeAdjacentOutNode(String edgeLabel,
                                    VertexRef<OverflowDbNode> nodeRef,
                                    Map<String, Object> edgeKeyValues) {
    storeAdjacentNode(Direction.OUT, edgeLabel, nodeRef);
  }

  private void storeAdjacentInNode(String edgeLabel, VertexRef<OverflowDbNode> nodeRef) {
    storeAdjacentNode(Direction.IN, edgeLabel, nodeRef);
  }

  private void storeAdjacentNode(Direction direction, String edgeLabel, VertexRef<OverflowDbNode> nodeRef) {
    int offsetPos = getPositionInEdgeOffsets(direction, edgeLabel);
    int start = startIndex(offsetPos);
    int length = blockLength(offsetPos);
    int strideSize = getEdgeKeyCount(edgeLabel) + 1;

    adjacentVerticesWithProperties = insertInNewArray(adjacentVerticesWithProperties, start + length, nodeRef, strideSize);

    // Increment length.
    edgeOffsets[2 * offsetPos + 1] = length + strideSize;

    // Increment all following start offsets by strideSize.
    for (int i = offsetPos + 1; 2 * i < edgeOffsets.length; i++) {
      edgeOffsets[2 * i] = edgeOffsets[2 * i] + strideSize;
    }
  }

  /**
   * returns a new array with the given `element` inserted at index `insertAt`,
   * occupying enough space for itself and it's properties (`strideSize`)
   */
  private Object[] insertInNewArray(Object array[], int insertAt, VertexRef element, int strideSize) {
    Object[] newArray = new Object[array.length + strideSize];

    System.arraycopy(array, 0, newArray, 0, insertAt);
    newArray[insertAt] = element;
    System.arraycopy(array, insertAt, newArray, insertAt + strideSize, array.length - insertAt);

    return newArray;
  }

  private int startIndex(int offsetPosition) {
    return edgeOffsets[2 * offsetPosition];
  }

  /**
   * Returns the length of an edge type block in the adjacentVerticesWithProperties array.
   * Length means number of index positions.
   */
  private int blockLength(int offsetPosition) {
    return edgeOffsets[2 * offsetPosition + 1];
  }

  private Labels calculateInOutLabelsToFollow(Direction direction, String... edgeLabels) {
    final Set<String> inEdgeLabels;
    final Set<String> outEdgeLabels;
    if (edgeLabels.length == 0) { // follow all labels
      switch (direction) {
        case IN:
          inEdgeLabels = allowedInEdgeLabels();
          outEdgeLabels = new THashSet<>(0);
          break;
        case OUT:
          inEdgeLabels = new THashSet<>(0);
          outEdgeLabels = allowedOutEdgeLabels();
          break;
        case BOTH:
          inEdgeLabels = allowedInEdgeLabels();
          outEdgeLabels = allowedOutEdgeLabels();
          break;
        default:
          throw new IllegalStateException("this will never happen - only needed to make the compiler happy");
      }
    } else { // follow specific labels
      inEdgeLabels = new THashSet<>();
      outEdgeLabels = new THashSet<>();
      for (String label : edgeLabels) {
        if (direction == Direction.IN || direction == Direction.BOTH) {
          inEdgeLabels.add(label);
        }
        if (direction == Direction.OUT || direction == Direction.BOTH) {
          outEdgeLabels.add(label);
        }
      }
    }

    return new Labels(inEdgeLabels, outEdgeLabels);
  }

  /**  to follow the tinkerpop api, instantiate and return a dummy edge, which doesn't really exist in the graph */
  protected OverflowDbEdge instantiateDummyEdge(String label, VertexRef<OverflowDbNode> outVertex, VertexRef<OverflowDbNode> inVertex) {
    final SpecializedElementFactory.ForEdge edgeFactory = graph.specializedEdgeFactoryByLabel.get(label);
    if (edgeFactory == null) throw new IllegalArgumentException("specializedEdgeFactory for label=" + label + " not found - please register on startup!");
    return (OverflowDbEdge)edgeFactory.createEdge(-1l, graph, outVertex, inVertex);
  }

  private Map<String, Object> toMap(Object[] keyValues) {
    final Map<String, Object> props = new HashMap<>(keyValues.length / 2);
    for (int i = 0; i < keyValues.length; i = i + 2) {
      if (!keyValues[i].equals(T.id) && !keyValues[i].equals(T.label)) {
        props.put((String) keyValues[i], keyValues[i + 1]);
      }
    }
    return props;
  }

  private class Labels {
    private final Set<String> forInEdges;
    private final Set<String> forOutEdges;

    public Labels(Set<String> forInEdges, Set<String> forOutEdges) {
      this.forInEdges = forInEdges;
      this.forOutEdges = forOutEdges;
    }
  }
}
