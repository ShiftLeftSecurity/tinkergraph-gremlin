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
 * Softly referencing element, so that the garbage collector can clear the element if we're running
 * low on heap space. That mechanism only works if there's no strong references to the element
 * elsewhere.
 * @see org.apache.tinkerpop.gremlin.tinkergraph.storage.SoftReferenceManager
 */
public abstract class ElementRef<E extends Element> implements Element {
  public final long id;
  public final String label;

  protected final TinkerGraph graph;
  protected SoftReference<E> softReference;

  public ElementRef(E element) {
    this.id = (long) element.id();
    this.label = element.label();
    this.graph = (TinkerGraph) element.graph();
    this.softReference = new SoftReference<>(element);
  }

  protected ElementRef(final long id, final String label, final TinkerGraph graph) {
    this.id = id;
    this.label = label;
    this.graph = graph;
  }

  public E get() {
    final E fromSoftRef = softReference.get();
    if (fromSoftRef != null) {
      return fromSoftRef;
    } else {
      try {
        final E element = readFromDisk(id, 5);
        this.softReference = new SoftReference<>(element);
        return element;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected E readFromDisk(long elementId, int maxRetries) throws Exception {
    final E element = readFromDisk(elementId);
    if (element != null) {
      return element;
    } else {
      String errorMsg = "Edge case with finalizer invocation order occurred: reference removed before `finalize` " +
              "was called, i.e. we're unable to read element with id=" + id + "from disk.";
      if (maxRetries > 0) {
        System.out.println(errorMsg + " Will sleep 100ms for the GC to call `finalize` and try again. maxRetries=" + maxRetries);
        // ugly workaround - TODO use callback instead to make this less ugly
        Thread.sleep(100);
        return readFromDisk(elementId, maxRetries - 1);
      } else {
        throw new RuntimeException(errorMsg + "max retries reached - aborting");
      }
    }
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
