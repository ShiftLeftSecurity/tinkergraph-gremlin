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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
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
 * TODO: extend Vertex (rather than SpecializedTinkerVertex)
 */
public abstract class OverflowDbNode extends SpecializedTinkerVertex {

  private VertexRef[] adjacentNodes = new VertexRef[0];
  // nodeoffsets stored the start offset and length into adjacentNodes array in an interleaved manner.
  private final int[] nodeOffsets;

  /**
   * @param numberOfDifferentAdjacentTypes The number fo different in and outgoing
   *                                       edge relations. E.g. a node has AST edges in and out,
   *                                       than we would have 2. If in addition it has incoming
   *                                       ref edges it would have 3.
   *
   */
  protected OverflowDbNode(long id, TinkerGraph graph, int numberOfDifferentAdjacentTypes) {
    super(id, graph);
    nodeOffsets = new int[numberOfDifferentAdjacentTypes * 2];
  }

  /**
   * @param direction
   * @param label
   * @return The position in nodeOffsets array. If the edge label is not supported, -1 need to be
   *         returned.
   */
  protected abstract int getPositionInNodeOffsets(Direction direction, String label);

  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    final VertexRef<OverflowDbNode> inVertexRef;
    if (inVertex instanceof VertexRef) inVertexRef = (VertexRef<OverflowDbNode>) inVertex;
    else inVertexRef = (VertexRef<OverflowDbNode>) graph.vertex((Long) inVertex.id());
    OverflowDbNode inVertexOdb = inVertexRef.get();
    VertexRef<OverflowDbNode> thisVertexRef = (VertexRef) graph.vertex((Long) id());

    storeAdjacentOutNode(label, inVertexRef, toMap(keyValues));
    inVertexOdb.storeAdjacentInNode(label, thisVertexRef);

    SpecializedTinkerEdge dummyEdge = instantiateDummyEdge(label, thisVertexRef, inVertexRef);
    ElementHelper.attachProperties(dummyEdge, keyValues);
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
    int offsetPos = getPositionInNodeOffsets(direction, label);
    if (offsetPos != -1) {
      int start = nodeOffsets[2 * offsetPos];
      int length = nodeOffsets[2 * offsetPos + 1];

      return new ArrayOffsetIterator<>(adjacentNodes, start, start + length);
    } else {
      return Collections.emptyIterator();
    }
  }

  private static class ArrayOffsetIterator<T> implements Iterator<T>  {
    private T[] array;
    private int current;
    private int end;

    ArrayOffsetIterator(T[] array, int begin, int end) {
      this.array = array;
      this.current = begin;
      this.end = end;
    }

    @Override
    public boolean hasNext() {
      return current < end;
    }

    @Override
    public T next() {
      T element = array[current];
      current += 1;
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
    int offsetPos = getPositionInNodeOffsets(direction, edgeLabel);
    int start = nodeOffsets[2 * offsetPos];
    int length = nodeOffsets[2 * offsetPos + 1];

    adjacentNodes = insertInNewArray(adjacentNodes, start + length, nodeRef);

    // Increment length.
    nodeOffsets[2 * offsetPos + 1] = length + 1;

    // Increment all following start offsets by one.
    for (int i = offsetPos + 1; 2 * i < nodeOffsets.length; i++) {
      nodeOffsets[2 * i] = nodeOffsets[2 * i] + 1;
    }
  }

  private VertexRef[] insertInNewArray(VertexRef array[], int insertAt, VertexRef element) {
    VertexRef[] newArray = new VertexRef[array.length + 1];
    for (int i = 0; i < insertAt; i++) {
      newArray[i] = array[i];
    }
    newArray[insertAt] = element;
    for (int i = insertAt; i < array.length; i++) {
      newArray[i + 1] = array[i];
    }

    return newArray;
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
  protected SpecializedTinkerEdge instantiateDummyEdge(String label, VertexRef<OverflowDbNode> outVertex, VertexRef<OverflowDbNode> inVertex) {
    final SpecializedElementFactory.ForEdge edgeFactory = graph.specializedEdgeFactoryByLabel.get(label);
    if (edgeFactory == null) throw new IllegalArgumentException("specializedEdgeFactory for label=" + label + " not found - please register on startup!");
    return edgeFactory.createEdge(-1l, graph, outVertex, inVertex);
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
