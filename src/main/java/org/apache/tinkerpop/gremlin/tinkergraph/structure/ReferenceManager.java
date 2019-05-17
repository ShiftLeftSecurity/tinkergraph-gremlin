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
  private final float heapUsageThreshold; // 1.0 == 100%
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
    heapUsageThreshold =  (float) heapPercentageThreshold / 100f;
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

  // TODO MP: run this asynchronously to not block the gc notification thread - use executor with one thread and capacity=1, drop `clearingInProgress` flag
  protected void maybeClearReferences(final float heapUsage) {
    if (heapUsage > heapUsageThreshold) {
      if (clearingInProgress) {
        System.out.println("cleaning currently in progress, not queuing to clear any more");
      } else if (clearableRefs.isEmpty()) {
        System.out.println("clearableRefs queue is empty - nothing to clear at the moment");
      } else {
        int releaseCount = Integer.min(this.releaseCount, clearableRefs.size());
        System.out.println("heap usage (after GC) is " + heapUsage + " -> clearing " + releaseCount + " references");
        final int actualReleaseCount = safelyClearReferences(releaseCount);
        totalReleaseCount += actualReleaseCount;
        System.out.println("completed clearing of "+ actualReleaseCount + " references");
        System.out.println("current clearable queue size: " + clearableRefs.size());
        System.out.println("references cleared in total: " + totalReleaseCount);
      }
    }
  }

  /**
   * clear references, ensuring no exception is raised
   *
   * @param releaseCount
   * @return number of references cleared
   */
  protected int safelyClearReferences(final int releaseCount) {
    try {
      clearingInProgress = true;
      return clearReferences(releaseCount);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      clearingInProgress = false;
    }
    return 0;
  }

  /**
   * @param releaseCount
   * @return number of references cleared
   */
  protected int clearReferences(int releaseCount) {
    ElementRef ref = clearableRefs.poll();
    int actualReleaseCount = 0;
    while (ref != null && releaseCount > 0) {
      if (ref.isSet()) {
        ref.clear();
        releaseCount--;
        actualReleaseCount++;
      }
      ref = clearableRefs.poll();
    }
    return actualReleaseCount;
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
          float heapUsage = (float) totalMemUsed / (float) totalMemMax;
          maybeClearReferences(heapUsage);
        }
      };
      NotificationEmitter emitter = (NotificationEmitter) gcbean;
      emitter.addNotificationListener(listener, null, null);
    }
  }

}
