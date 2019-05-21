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

import java.io.IOException;
import java.lang.ref.SoftReference;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Wrapper for an element reference, which may be set to `null` by @ReferenceManager to avoid OutOfMemory errors.
 * When it's cleared, it will be persisted to an on-disk storage.
 */
public abstract class ElementRef<E extends TinkerElement> implements Element {
  public final long id;
  public final String label;

  protected final TinkerGraph graph;
  protected E reference;
  private int serializationCount;
  private long lastDeserializedTime;
  private boolean removed = false;

  public ElementRef(E element) {
    this.id = (long) element.id();
    this.label = element.label();
    this.graph = (TinkerGraph) element.graph();
    this.reference = element;
    this.lastDeserializedTime = System.currentTimeMillis();
    graph.referenceManager.registerRef(this);
  }

  protected ElementRef(final long id, final String label, final TinkerGraph graph) {
    this.id = id;
    this.label = label;
    this.graph = graph;
  }

  public boolean isSet() {
    return reference != null;
  }

  public boolean isCleared() {
    return reference == null;
  }

  public boolean isRemoved() {
    return removed;
  }

  /* only called by @ReferenceManager */
  protected void clear() throws IOException {
    E ref = reference;
    if (ref != null) {
      graph.ondiskOverflow.persist(ref);
      serializationCount++;
    }
    reference = null;
  }

  public E get() {
    E ref = reference;
    if (ref != null) {
      return ref;
    } else {
      try {
        final E element = readFromDisk(id);
        this.reference = element;
        this.lastDeserializedTime = System.currentTimeMillis();
        graph.referenceManager.registerRef(this);
        return element;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public int getSerializationCount() {
    return serializationCount;
  }

  public long getLastDeserializedTime() {
    return lastDeserializedTime;
  }

  protected abstract E readFromDisk(long elementId) throws IOException;

  @Override
  public Object id() {
    return id;
  }

  @Override
  public Graph graph() {
    return graph;
  }

  @Override
  public String label() {
    return label;
  }

  // delegate methods start

  @Override
  public void remove() {
    this.removed = true;
    this.get().remove();
  }

  @Override
  public int hashCode() {
    return id().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof ElementRef) {
      return id().equals(((ElementRef) obj).id());
    } else {
      return false;
    }
  }
  // delegate methods end

}
