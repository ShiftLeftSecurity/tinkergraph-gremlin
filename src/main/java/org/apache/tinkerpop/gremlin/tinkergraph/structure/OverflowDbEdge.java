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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Set;

public abstract class OverflowDbEdge extends SpecializedTinkerEdge {
  private final TinkerGraph graph;
  private final String label;
  private final VertexRef<OverflowDbNode> outVertex;
  private final VertexRef<OverflowDbNode> inVertex;
  private int outBlockOffset = UNITIALIZED_BLOCK_OFFSET;
  private int inBlockOffset = UNITIALIZED_BLOCK_OFFSET;

  private static final int UNITIALIZED_BLOCK_OFFSET = -1;

  public OverflowDbEdge(TinkerGraph graph,
                        String label,
                        VertexRef<OverflowDbNode> outVertex,
                        VertexRef<OverflowDbNode> inVertex,
                        Set<String> specificKeys) {
    super(graph, -1L, outVertex, label, inVertex, specificKeys);
    this.graph = graph;
    this.label = label;
    this.outVertex = outVertex;
    this.inVertex = inVertex;
  }

  public int getOutBlockOffset() {
    return outBlockOffset;
  }

  public void setOutBlockOffset(int offset) {
    outBlockOffset = offset;
  }

  public int getInBlockOffset() {
    return inBlockOffset;
  }

  public void setInBlockOffset(int offset) {
    inBlockOffset = offset;
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction) {
    switch (direction) {
      case OUT:
        return IteratorUtils.of(outVertex);
      case IN:
        return IteratorUtils.of(inVertex);
      default:
        return IteratorUtils.of(outVertex, inVertex);
    }
  }

  @Override
  public Object id() {
    return -1;
  }

  @Override
  public String label() {
    return label;
  }

  @Override
  protected <V> Property<V> specificProperty(String key) {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public Graph graph() {
    return graph;
  }

  @Override
  public <V> Property<V> property(String key, V value) {
    // TODO check if it's an allowed property key
    if (inBlockOffset != UNITIALIZED_BLOCK_OFFSET) {
      if (outBlockOffset == UNITIALIZED_BLOCK_OFFSET) {
        int numberOfEdgesWithSameLabelBetweenInAndOutVertex =
            inVertex.get().blockOffsetToOccurrence(Direction.IN, label(), outVertex, inBlockOffset);
        outBlockOffset = outVertex.get().occurrenceToBlockOffset(Direction.OUT, label(), inVertex,
            numberOfEdgesWithSameLabelBetweenInAndOutVertex);
      }
    } else if (outBlockOffset != UNITIALIZED_BLOCK_OFFSET) {
      if (inBlockOffset == UNITIALIZED_BLOCK_OFFSET) {
        int numberOfEdgesWithSameLabelBetweenInAndOutVertex =
            outVertex.get().blockOffsetToOccurrence(Direction.OUT, label(), inVertex, outBlockOffset);
        inBlockOffset = inVertex.get().occurrenceToBlockOffset(Direction.IN, label(), outVertex,
            numberOfEdgesWithSameLabelBetweenInAndOutVertex);
      }
    } else {
      throw new RuntimeException("Cannot set property. In and out block offset unitialized.");
    }
    inVertex.get().setEdgeProperty(Direction.IN, label, key, value, inBlockOffset);
    outVertex.get().setEdgeProperty(Direction.OUT, label, key, value, outBlockOffset);
    return new OverflowProperty<>(key, value, this);
  }

  @Override
  protected <V> Property<V> updateSpecificProperty(String key, V value) {
    throw new RuntimeException("Not supported.");
  }

  @Override
  protected void removeSpecificProperty(String key) {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not supported.");
  }

  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    if (inBlockOffset != -1) {
      return inVertex.get().getEdgeProperties(Direction.IN, this, getInBlockOffset(), propertyKeys);
    } else if (outBlockOffset != -1) {
      return outVertex.get().getEdgeProperties(Direction.OUT, this, getOutBlockOffset(), propertyKeys);
    } else {
      throw new RuntimeException("Cannot get properties. In and out block offset unitialized.");
    }
  }

  @Override
  public <V> Property<V> property(String propertyKey) {
    if (inBlockOffset != -1) {
      return inVertex.get().getEdgeProperty(Direction.IN, this, inBlockOffset, propertyKey);
    } else if (outBlockOffset != -1) {
      return outVertex.get().getEdgeProperty(Direction.OUT, this, outBlockOffset, propertyKey);
    } else {
      throw new RuntimeException("Cannot get property. In and out block offset unitialized.");
    }
  }

}
