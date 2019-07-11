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
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyProperty;
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

  private Object[] adjacentVerticesWithProperties = new Object[0];

  /* store the start offset and length into the above `adjacentVerticesWithProperties` array in an interleaved manner,
   * i.e. each outgoing edge type has two entries in this array. */
  private final int[] edgeOffsets;

  /* determines how many spaces for adjacent vertices will be left free, so we don't need to grow the array for every additional edge */
  private final int growthEmptyFactor = 3; // TODO make configurable

  /**
   * @param numberOfDifferentAdjacentTypes The number fo different IN|OUT edge relations. E.g. a node has AST edges in
   *                                       and out, then we would have 2. If in addition it has incoming
   *                                       ref edges it would have 3.
   */
  protected OverflowDbNode(int numberOfDifferentAdjacentTypes,
                           VertexRef<Vertex> ref) {
    super(ref);
    edgeOffsets = new int[numberOfDifferentAdjacentTypes * 2];
  }

  /**
   * @return The position in edgeOffsets array. -1 if the edge label is not supported
   */
  protected abstract int getPositionInEdgeOffsets(Direction direction, String label);

  /**
   *
   * @return The offset relative to the adjacent vertex element in the
   * adjacentVerticesWithProperties array starting from 1. Return -1 if
   * key does not exist for given edgeLabel.
   */
  protected abstract int getOffsetRelativeToAdjacentVertexRef(String edgeLabel, String key);

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

  public <V> Property<V> getEdgeProperty(String edgeLabel,
                                         String key,
                                         VertexRef<OverflowDbNode> inVertex) {
    int propertyPosition = getEdgePropertyIndex(edgeLabel, key, inVertex);
    if (propertyPosition == -1) {
      return EmptyProperty.instance();
    }
    V value = (V) adjacentVerticesWithProperties[propertyPosition];
    if (value == null) {
      return EmptyProperty.instance();
    }
    VertexRef<OverflowDbNode> thisVertexRef = (VertexRef) ref.graph.vertex((Long) id());
    // For now we do not create and set a dummy edge on the edge property
    // in order to save the associated overhead. Seems to not be required
    // be tinkerpop core, lets see whether this is true.
    OverflowDbEdge edge = null; //instantiateDummyEdge(edgeLabel, thisVertexRef, inVertex);
    return new OverflowProperty<>(key, value, edge);
  }

  public <V> Property<V> setEdgeProperty(String edgeLabel,
                                         String key,
                                         V value,
                                         VertexRef<OverflowDbNode> inVertex,
                                         OverflowDbEdge edge) {
    int propertyPosition = getEdgePropertyIndex(edgeLabel, key, inVertex);
    if (propertyPosition == -1) {
      throw new RuntimeException("Property " + key + " not support on edge " + edgeLabel + ".");
    }
    adjacentVerticesWithProperties[propertyPosition] = value;
    return new OverflowProperty<>(key, value, edge);
  }

  /**
   * Return -1 if there exists no edge property for the provided argument combination.
   */
  private int getEdgePropertyIndex(String label, String key, VertexRef<OverflowDbNode> inVertex) {
    int offsetPos = getPositionInEdgeOffsets(Direction.OUT, label);
    if (offsetPos == -1) {
      return -1;
    }
    int start = startIndex(offsetPos);
    int length = blockLength(offsetPos);
    int strideSize = getEdgeKeyCount(label) + 1;
    int propertyOffset = getOffsetRelativeToAdjacentVertexRef(label, key);
    if (propertyOffset == -1) {
      return -1;
    }

    for (int i = start; i < start + length; i += strideSize) {
      if (((VertexRef)adjacentVerticesWithProperties[i]).id().equals(inVertex.id())) {
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
    else inVertexRef = (VertexRef<OverflowDbNode>) ref.graph.vertex((Long) inVertex.id());
    OverflowDbNode inVertexOdb = inVertexRef.get();
    VertexRef<OverflowDbNode> thisVertexRef = (VertexRef) ref.graph.vertex((Long) id());

    storeAdjacentOutNode(label, inVertexRef, keyValues);
    inVertexOdb.storeAdjacentInNode(label, thisVertexRef);

    OverflowDbEdge dummyEdge = instantiateDummyEdge(label, thisVertexRef, inVertexRef);
    return dummyEdge;
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    final Labels labels = calculateInOutLabelsToFollow(direction, edgeLabels);
    final MultiIterator2<Edge> multiIterator = new MultiIterator2<>();
    VertexRef thisRef = (VertexRef) ref.graph.vertex(ref.id);
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
                                    Object... edgeKeyValues) {
    storeAdjacentNode(Direction.OUT, edgeLabel, nodeRef);
    VertexRef<OverflowDbNode> thisVertexRef = null;
    OverflowDbEdge dummyEdge = null;
    for (int i = 0; i < edgeKeyValues.length; i = i + 2) {
      if (!edgeKeyValues[i].equals(T.id) && !edgeKeyValues[i].equals(T.label)) {
        String key = (String) edgeKeyValues[i];
        Object value = edgeKeyValues[i + 1];
        if (thisVertexRef == null) {
          thisVertexRef = (VertexRef) ref.graph.vertex((Long) id());
          // For now we do not create and set a dummy edge on the edge property
          // in order to save the associated overhead. Seems to not be required
          // be tinkerpop core, lets see whether this is true.
          //dummyEdge = instantiateDummyEdge(edgeLabel, thisVertexRef, nodeRef);
        }
        setEdgeProperty(edgeLabel, key, value, nodeRef, dummyEdge);
      }
    }
  }

  private void storeAdjacentInNode(String edgeLabel, VertexRef<OverflowDbNode> nodeRef) {
    storeAdjacentNode(Direction.IN, edgeLabel, nodeRef);
  }

  private void storeAdjacentNode(Direction direction, String edgeLabel, VertexRef<OverflowDbNode> nodeRef) {
    int offsetPos = getPositionInEdgeOffsets(direction, edgeLabel);
    if (offsetPos == -1) {
      throw new RuntimeException("Edge of type " + edgeLabel + " with direction " + direction +
          " not supported by class " + getClass().getSimpleName());
    }
    int start = startIndex(offsetPos);
    int length = blockLength(offsetPos);
    int strideSize = getEdgeKeyCount(edgeLabel) + 1;

    int insertAt = start + length;
    if (adjacentVerticesWithProperties.length <= insertAt || adjacentVerticesWithProperties[insertAt] != null) {
      // space already occupied - grow adjacentVerticesWithProperties array, leaving some room for more elements
      adjacentVerticesWithProperties = growAdjacentVerticesWithProperties(offsetPos, strideSize, insertAt);;
    }

    adjacentVerticesWithProperties[insertAt] = nodeRef;
    // update edgeOffset length to include the newly inserted element
    edgeOffsets[2 * offsetPos + 1] = length + strideSize;
  }

  /**
   * grow the adjacentVerticesWithProperties array
   *
   * preallocates more space than immediately necessary, so we don't need to grow the array every time
   * (tradeoff between performance and memory)
   */
  private Object[] growAdjacentVerticesWithProperties(int offsetPos, int strideSize, int insertAt) {
    int additionalEntriesCount = strideSize * growthEmptyFactor;
    int newSize = adjacentVerticesWithProperties.length + additionalEntriesCount;
    Object[] newArray = new Object[newSize];
    System.arraycopy(adjacentVerticesWithProperties, 0, newArray, 0, insertAt);
    System.arraycopy(adjacentVerticesWithProperties, insertAt, newArray, insertAt + additionalEntriesCount, adjacentVerticesWithProperties.length - insertAt);

    // Increment all following start offsets by `additionalEntriesCount`.
    for (int i = offsetPos + 1; 2 * i < edgeOffsets.length; i++) {
      edgeOffsets[2 * i] = edgeOffsets[2 * i] + additionalEntriesCount;
    }
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
    final SpecializedElementFactory.ForEdge edgeFactory = ref.graph.specializedEdgeFactoryByLabel.get(label);
    if (edgeFactory == null) throw new IllegalArgumentException("specializedEdgeFactory for label=" + label + " not found - please register on startup!");
    return (OverflowDbEdge)edgeFactory.createEdge(-1l, ref.graph, outVertex, inVertex);
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
