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

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OverflowDbTestNode extends OverflowDbNode implements Serializable {
  public static final String label = "testNode";

  public static final String STRING_PROPERTY = "StringProperty";
  public static final String INT_PROPERTY = "IntProperty";
  public static final String STRING_LIST_PROPERTY = "StringListProperty";
  public static final String INT_LIST_PROPERTY = "IntListProperty";

  private static final Map<String, Integer> outEdgeToPosition = new HashMap<>();
  private static final Map<String, Integer> inEdgeToPosition = new HashMap<>();
  private static final Map<String, Integer> labelAndKeyToPosition = new HashMap<>();
  private static final Map<String, Integer> edgeKeyCount = new HashMap<>();

  static {
    outEdgeToPosition.put(OverflowDbTestEdge.label, 0);
    inEdgeToPosition.put(OverflowDbTestEdge.label, 1);
    labelAndKeyToPosition.put(OverflowDbTestEdge.label + STRING_PROPERTY, 0);
    labelAndKeyToPosition.put(OverflowDbTestEdge.label + INT_PROPERTY, 1);
    labelAndKeyToPosition.put(OverflowDbTestEdge.label + STRING_LIST_PROPERTY, 2);
    labelAndKeyToPosition.put(OverflowDbTestEdge.label + INT_LIST_PROPERTY, 3);
    edgeKeyCount.put(OverflowDbTestEdge.label, 4);
  }

  public OverflowDbTestNode(Long id, TinkerGraph graph) {
    super(id, graph, outEdgeToPosition.size() + inEdgeToPosition.size());
  }

  @Override
  protected int getPositionInEdgeOffsets(Direction direction, String label) {
    final Integer positionOrNull;
    if (direction == Direction.OUT) {
      positionOrNull = outEdgeToPosition.get(label);
    } else {
      positionOrNull = inEdgeToPosition.get(label);
    }
    if (positionOrNull != null) {
      return positionOrNull;
    } else {
      return -1;
    }
  }

  @Override
  protected int getOffsetRelativeToAdjacentVertexRef(String edgeLabel, String key) {
    // TODO handle if it's not allowed
    return labelAndKeyToPosition.get(edgeLabel + key);
  }

  @Override
  protected int getEdgeKeyCount(String edgeLabel) {
    // TODO handle if it's not allowed
    return edgeKeyCount.get(edgeLabel);
  }

  public static SpecializedElementFactory.ForVertex<OverflowDbTestNode> factory = new SpecializedElementFactory.ForVertex<OverflowDbTestNode>() {
    @Override
    public String forLabel() {
      return OverflowDbTestNode.label;
    }

    @Override
    public OverflowDbTestNode createVertex(Long id, TinkerGraph graph) {
      return new OverflowDbTestNode(id, graph);
    }

    @Override
    public VertexRef<OverflowDbTestNode> createVertexRef(OverflowDbTestNode vertex) {
      return new VertexRefWithLabel<>(vertex.id(), vertex.graph, vertex, OverflowDbTestNode.label);
    }

    @Override
    public VertexRef<OverflowDbTestNode> createVertexRef(Long id, TinkerGraph graph) {
      return new VertexRefWithLabel<>(id, graph, null, OverflowDbTestNode.label);
    }
  };

  @Override
  public String label() {
    return OverflowDbTestNode.label;
  }

}
