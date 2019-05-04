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
package org.apache.tinkerpop.gremlin.tinkergraph.storage;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;

/**
 * By default, the JVM will clear *all* soft references when it runs low on memory.
 * That's a waste of resources, because they'll need to be deserialized back from disk, which is expensive.
 * Instead, we want the GC to only clear a small percentage, and therefor `SoftReferenceManager` holds onto the rest via strong references.
 */
public class SoftReferenceManager {
  public final int maxWildSoftReferences;
  protected int observedElementCount = 0;
  protected final ConcurrentLinkedDeque<TinkerElement> strongRefs = new ConcurrentLinkedDeque<>(); // `iterator.remove(Object)` is O(1)
  protected final OndiskOverflow ondiskOverflow;

  /**
   * @param maxWildSoftReferences
   * maximum number of elements that can remain softly referenced "in the wild", i.e. we won't hold a strong reference to them
   */
  public SoftReferenceManager(final int maxWildSoftReferences, final OndiskOverflow ondiskOverflow) {
    this.maxWildSoftReferences = maxWildSoftReferences;
    this.ondiskOverflow = ondiskOverflow;
  }

  /** called from Element's constructor */
  public void register(TinkerElement element) {
    observedElementCount++;
    if (observedElementCount > maxWildSoftReferences) {
      // hold onto a strong reference to this element, so that it doesn't get cleared by the GC when we're low on memory
      // element will be added at the *end* of the queue
      strongRefs.add(element);
    }
  }

  /**
   * Called from `Element.finalize()`, i.e. either an element got normally finalized or it got
   * cleared by the GC because we're low on heap.
   * I.e. there are now potentially less 'only softly reachable elements' in the wild, so we should
   * free some strong references (if we have any).
   */
  public void notifyObjectFinalized(TinkerElement finalizedElement) {
    observedElementCount--;
    Iterator<TinkerElement> iterator = strongRefs.iterator();
    if (iterator.hasNext()) {
      iterator.next();
      iterator.remove(); // element is removed from the *beginning* of the queue
    }
    if (!finalizedElement.isRemoved()) {
      ondiskOverflow.persist(finalizedElement);
    }
  }
}
