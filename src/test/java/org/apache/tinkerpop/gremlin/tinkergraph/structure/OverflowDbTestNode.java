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
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OverflowDbTestNode extends OverflowDbNode implements Serializable {
  public static final String label = "testNode";

  public static final String STRING_PROPERTY = "StringProperty";
  public static final String INT_PROPERTY = "IntProperty";
  public static final String STRING_LIST_PROPERTY = "StringListProperty";
  public static final String INT_LIST_PROPERTY = "IntListProperty";
  public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(STRING_PROPERTY, INT_PROPERTY, STRING_LIST_PROPERTY, INT_LIST_PROPERTY));

  public static final Set<String> ALLOWED_IN_EDGE_LABELS = new HashSet<>(Arrays.asList(OverflowDbTestEdge.label));
  public static final Set<String> ALLOWED_OUT_EDGE_LABELS = new HashSet<>(Arrays.asList(OverflowDbTestEdge.label));

  private static final Map<String, Integer> outEdgeToPosition = new HashMap<>();
  private static final Map<String, Integer> inEdgeToPosition = new HashMap<>();

  // properties
  private String stringProperty;
  private Integer intProperty;
  private List<String> stringListProperty;
  private List<Integer> intListProperty;

  /* properties of edges between nodes
   * since we're only emulating edges, we're storing the edge properties in a separate list per edgetype and property
   * only stored on the SRC node side */
  protected final List<Object> testEdgeOutLongProperty = new ArrayList<>();

  static {
    outEdgeToPosition.put(OverflowDbTestEdge.label, 0);
    inEdgeToPosition.put(OverflowDbTestEdge.label, 1);
  }

  public OverflowDbTestNode(Long id, TinkerGraph graph) {
    super(id, graph, outEdgeToPosition.size() + inEdgeToPosition.size());
  }

  @Override
  protected int getPositionInNodeOffsets(Direction direction, String label) {
    Integer positionOrNull;
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
  protected Set<String> specificKeys() {
    return SPECIFIC_KEYS;
  }

  @Override
  public Set<String> allowedOutEdgeLabels() {
    return ALLOWED_OUT_EDGE_LABELS;
  }

  @Override
  public Set<String> allowedInEdgeLabels() {
    return ALLOWED_IN_EDGE_LABELS;
  }

  /* note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings */
  @Override
  protected <V> Iterator<VertexProperty<V>> specificProperties(String key) {
    final VertexProperty<V> ret;
    if (STRING_PROPERTY.equals(key) && stringProperty != null) {
      return IteratorUtils.of(new SpecializedVertexProperty(this, key, stringProperty));
    } else if (key == STRING_LIST_PROPERTY && stringListProperty != null) {
      return IteratorUtils.of(new SpecializedVertexProperty(this, key, stringListProperty));
    } else if (key == INT_PROPERTY && intProperty != null) {
      return IteratorUtils.of(new SpecializedVertexProperty(this, key, intProperty));
    } else if (key == INT_LIST_PROPERTY && intListProperty != null) {
      return IteratorUtils.of(new SpecializedVertexProperty(this, key, intListProperty));
    } else {
      return Collections.emptyIterator();
    }
  }

  @Override
  public Map<String, Object> valueMap() {
    Map<String, Object> properties = new HashMap<>();
    if (stringProperty != null) properties.put(STRING_PROPERTY, stringProperty);
    if (stringListProperty != null) properties.put(STRING_LIST_PROPERTY, stringListProperty);
    if (intProperty != null) properties.put(INT_PROPERTY, intProperty);
    if (intListProperty != null) properties.put(INT_LIST_PROPERTY, intListProperty);
    return properties;
  }

  @Override
  protected <V> VertexProperty<V> updateSpecificProperty(
      VertexProperty.Cardinality cardinality, String key, V value) {
    if (STRING_PROPERTY.equals(key)) {
      this.stringProperty = (String) value;
    } else if (STRING_LIST_PROPERTY.equals(key)) {
      if (value instanceof List) {
        this.stringListProperty = (List) value;
      } else {
        if (this.stringListProperty == null) this.stringListProperty = new ArrayList<>();
        this.stringListProperty.add((String) value);
      }
    } else if (INT_PROPERTY.equals(key)) {
      this.intProperty = (Integer) value;
    } else if (INT_LIST_PROPERTY.equals(key)) {
      if (value instanceof List) {
        this.intListProperty = (List) value;
      } else {
        if (this.intListProperty == null) this.intListProperty = new ArrayList<>();
        this.intListProperty.add((Integer) value);
      }
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
    return property(key);
  }

  @Override
  protected void removeSpecificProperty(String key) {
    if (STRING_PROPERTY.equals(key)) {
      this.stringProperty = null;
    } else if (STRING_LIST_PROPERTY.equals(key)) {
      this.stringListProperty = null;
    } else if (INT_PROPERTY.equals(key)) {
      this.intProperty = null;
    } else if (INT_LIST_PROPERTY.equals(key)) {
      this.intListProperty = null;
    } else {
      throw new RuntimeException("property with key=" + key + " not (yet) supported by " + this.getClass().getName());
    }
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
