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

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** watches GC activity, and when we're low on available heap space,
 * it sets some references in the `[Vertex|Edge]Refs` to `null`, to avoid OOM
 * */
public class ReferenceManager {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private Map<NotificationEmitter, NotificationListener> gcNotificationListeners = new HashMap<>(2);
  private final float heapUsageThreshold; // range 0.0 - 1.0
  public final int backpressureMillis = 500; //TODO make configurable
  public final int releaseCount = 100000; //TODO make configurable
  private int totalReleaseCount;
  private final int cpuCount = Runtime.getRuntime().availableProcessors();
  private final ExecutorService executorService = Executors.newFixedThreadPool(cpuCount);
  private final AtomicInteger clearingProcessCount = new AtomicInteger(0);

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
    if (heapPercentageThreshold < 0 || heapPercentageThreshold > 100) {
      throw new IllegalArgumentException("heapPercentageThreshold must be between 0 and 100, but is " + heapPercentageThreshold);
    }
    heapUsageThreshold =  (float) heapPercentageThreshold / 100f;
    installGCMonitoring();
  }

  public void registerRef(ElementRef ref) {
    clearableRefs.add(ref);
  }

  /** when we're running low on heap memory we'll serialize some elements to disk. to ensure we're not creating new ones
   * faster than old ones are serialized away, we're applying some backpressure in those situation */
  public void applyBackpressureMaybe() {
    if (clearingProcessCount.get() > 0) {
      try {
        logger.trace("applying " + backpressureMillis + "ms backpressure");
        Thread.sleep(backpressureMillis);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected void maybeClearReferences(final float heapUsage) {
    if (heapUsage > heapUsageThreshold) {
      if (clearingProcessCount.get() > 0) {
        logger.debug("cleaning in progress, will only queue up more references to clear after that's completed");
      } else if (clearableRefs.isEmpty()) {
        logger.debug("clearableRefs queue is empty - nothing to clear at the moment");
      } else {
        int releaseCount = Integer.min(this.releaseCount, clearableRefs.size());
        logger.info("heap usage (after GC) was " + heapUsage + " -> scheduled to clear " + releaseCount + " references (asynchronously)");
        asynchronouslyClearReferences(releaseCount);
      }
    }
  }

  /** run clearing of references asynchronously to not block the gc notification thread
  *using executor with one thread and capacity=1, drop `clearingInProgress` flag */
  protected void asynchronouslyClearReferences(final int releaseCount) {
    for (int i = 0; i < cpuCount; i++) {
      final int releaseCountPerThread = releaseCount / cpuCount;
      executorService.submit(() -> {
        final int actualReleaseCount = safelyClearReferences(releaseCountPerThread);
        totalReleaseCount += actualReleaseCount;
        logger.info("completed clearing of "+ actualReleaseCount + " references");
        logger.debug("current clearable queue size: " + clearableRefs.size());
        logger.debug("references cleared in total: " + totalReleaseCount);
      });
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
      clearingProcessCount.incrementAndGet();
      return clearReferences(releaseCount);
    } catch (Exception e) {
      logger.error("error while trying to clear " + releaseCount + " references", e);
    } finally {
      clearingProcessCount.decrementAndGet();
    }
    return 0;
  }

  /**
   * @param releaseCount
   * @return number of references cleared
   */
  protected int clearReferences(int releaseCount) throws IOException {
    logger.info("attempting to clear "+ releaseCount + " references");
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
    List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean gcbean : gcbeans) {
      NotificationListener listener = createNotificationListener();
      NotificationEmitter emitter = (NotificationEmitter) gcbean;
      emitter.addNotificationListener(listener, null, null);
      gcNotificationListeners.put(emitter, listener);
    }
    int heapUsageThresholdPercent = (int) Math.floor(heapUsageThreshold * 100f);
    logger.info("installed GC monitors. will clear references if heap (after GC) is larger than " + heapUsageThresholdPercent + "%");
  }

  private NotificationListener createNotificationListener() {
    Set<String> ignoredMemoryAreas = new HashSet<>(Arrays.asList("Code Cache", "Compressed Class Space", "Metaspace"));
    return (notification, handback) -> {
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
            int heapUsagePercent = (int) Math.floor(heapUsage * 100f);
            logger.debug("heap usage after GC: " + heapUsagePercent + "%");
            maybeClearReferences(heapUsage);
          }
        };
  }

  protected void uninstallGCMonitoring() {
    for (Map.Entry<NotificationEmitter, NotificationListener> entry : gcNotificationListeners.entrySet()) {
      try {
        entry.getKey().removeNotificationListener(entry.getValue());
      } catch (ListenerNotFoundException e) {
        throw new RuntimeException("unable to remove GC monitor", e);
      }
    }
    logger.info("uninstalled GC monitors.");
  }

  public void close() {
    uninstallGCMonitoring();
    executorService.shutdown();
  }
}
