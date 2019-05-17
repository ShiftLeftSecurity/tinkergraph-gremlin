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

import com.sun.management.GarbageCollectionNotificationInfo;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

/** watches GC activity, and when we're low on available heap space,
 * it sets some references in the `[Vertex|Edge]Refs` to `null`, to avoid OOM
 * */
public class ReferenceManager {
  private final int heapPercentageThreshold;
  public final int releaseCount = 100000; //TODO make configurable
  private int totalReleaseCount;
  private boolean clearingInProgress = false;

  /** prioritize references by
   * 1) serializationCount, i.e. elements that have been serialized more often will be serialized later
   * 2) lastDeserializationTime, i.e. elements that have more recently been deserialized will be serialized last
   * */
  private final Comparator<ElementRef> refComparator = (ref1, ref2) -> {
    if (ref1.getSerializationCount() != ref2.getSerializationCount()) {
      return ref1.getSerializationCount() < ref2.getSerializationCount() ? -1 : 1;
    } else {
      return ref1.getLastDeserializedTime() < ref2.getLastDeserializedTime() ? -1 : 1;
    }
  };
  private final PriorityBlockingQueue<ElementRef> clearableRefs = new PriorityBlockingQueue<>(100000, refComparator);

  public ReferenceManager(int heapPercentageThreshold) {
    this.heapPercentageThreshold = heapPercentageThreshold;
    installGCMonitoring();
  }

  public void registerRef(ElementRef ref) {
    clearableRefs.add(ref);
  }

  /** when we're running low on heap memory we'll serialize some elements to disk. to ensure we're not creating new ones
   * faster than old ones are serialized away, we're applying some backpressure in those situation */
  public void applyBackpressureMaybe() {
    if (clearingInProgress) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected void safelyClearReferences() {
    if (clearingInProgress) {
      System.out.println("ReferenceManager: cleaning currently in progress, not queuing to clear any more");
    } else if (clearableRefs.isEmpty()) {
      System.out.println("ReferenceManager: clearableRefs queue is empty - nothing to clear at the moment");
    } else {
      try {
        clearingInProgress = true;
        clearReferences();
      } catch (Throwable t) {
        t.printStackTrace();
      } finally {
        clearingInProgress = false;
      }
    }
  }

  protected void clearReferences() {
    ElementRef ref = clearableRefs.poll();
    int releaseCount = Integer.min(this.releaseCount, clearableRefs.size());
    System.out.println("ReferenceManager: clearing " + releaseCount + " references");
    int actualReleaseCount = 0;
    while (ref != null && releaseCount > 0) {
      if (ref.isSet()) {
        ref.clear();
        releaseCount--;
        actualReleaseCount++;
      }
      ref = clearableRefs.poll();
    }
    totalReleaseCount += actualReleaseCount;
    System.out.println("ReferenceManager: completed clearing of "+ actualReleaseCount + " references (" + totalReleaseCount + " in total).");
  }

  /** monitor GC, and should the heap grow above 80% usage, clear some strong references */
  protected void installGCMonitoring() {
    // TODO use logging lib
    System.out.println("ReferenceManager.installGCMonitoring");
    Set<String> ignoredMemoryAreas = new HashSet<>(Arrays.asList("Code Cache", "Compressed Class Space", "Metaspace"));

    List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcbean : gcbeans) {
      NotificationListener listener = (notification, handback) -> {
        if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
          GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());

          //sum up used and max memory across relevant memory areas
          long totalMemUsed = 0;
          long totalMemMax = 0;
          for (Map.Entry<String, MemoryUsage> entry : info.getGcInfo().getMemoryUsageAfterGc().entrySet()) {
            String name = entry.getKey();
            if (!ignoredMemoryAreas.contains(name)) {
              MemoryUsage detail = entry.getValue();
              totalMemUsed += detail.getUsed();
              totalMemMax += detail.getMax();
            }
          }
          float heapUsageBefore = (float) totalMemUsed / (float) totalMemMax;
          float threshold = (float) heapPercentageThreshold / 100f;
          if (heapUsageBefore > threshold) {
            System.out.println("heap usage (after GC) is " + heapUsageBefore);
            safelyClearReferences();
          }
        }
      };
      NotificationEmitter emitter = (NotificationEmitter) gcbean;
      emitter.addNotificationListener(listener, null, null);
    }
  }

}
