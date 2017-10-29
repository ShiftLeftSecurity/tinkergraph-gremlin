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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedElementFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.SpecializedTinkerEdge;

import java.util.*;

public class FollowedBy extends SpecializedTinkerEdge {
    public static String label = "followedBy";

    public static String WEIGHT = "weight";
    public static Set<String> SPECIFIC_KEYS = new HashSet<>(Arrays.asList(WEIGHT));

    private final Integer weight;

    public FollowedBy(Object id, Vertex outVertex, Vertex inVertex, Integer weight) {
        super(id, outVertex, label, inVertex, SPECIFIC_KEYS);
        this.weight = weight;
    }

    @Override
    protected <V> V specificProperty(String key) {
        // note: usage of `==` (pointer comparison) over `.equals` (String content comparison) is intentional for performance - use the statically defined strings
        if (key == WEIGHT) {
            return (V) weight;
        } else {
            throw new NoSuchElementException(key);
        }
    }

    public static SpecializedElementFactory.ForEdge<FollowedBy> factory = new SpecializedElementFactory.ForEdge<FollowedBy>() {
        @Override
        public String forLabel() {
            return FollowedBy.label;
        }

        @Override
        public FollowedBy createEdge(Object id, Vertex outVertex, Vertex inVertex, Map<String, Object> keyValueMap) {
            Integer weight = (Integer) keyValueMap.get("weight");
            return new FollowedBy(id, outVertex, inVertex, weight);
        }
    };
}
