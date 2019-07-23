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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.specialized.gratefuldead;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowDbEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowDbNode;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.OverflowElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.VertexRef;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SungBy extends OverflowDbEdge {
  public static final String label = "sungBy";

  public static final Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList());

  public SungBy(TinkerGraph graph, VertexRef<OverflowDbNode> outVertex, VertexRef<OverflowDbNode> inVertex) {
    super(graph, label, outVertex, inVertex, SPECIFIC_KEYS);
  }

  public static OverflowElementFactory.ForEdge<SungBy> factory = new OverflowElementFactory.ForEdge<SungBy>() {
    @Override
    public String forLabel() {
      return SungBy.label;
    }

    @Override
    public SungBy createEdge(TinkerGraph graph, VertexRef outVertex, VertexRef inVertex) {
      return new SungBy(graph, outVertex, inVertex);
    }
  };
}
