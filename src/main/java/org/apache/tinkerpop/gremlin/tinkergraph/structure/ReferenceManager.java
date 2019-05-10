package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import com.sun.management.GarbageCollectionNotificationInfo;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/** watches GC activity, and when we're low on available heap space,
 * it sets some references in the `[Vertex|Edge]Refs` to `null`, to avoid OOM
 * */
public class ReferenceManager {
  private final int heapPercentageThreshold;
  private final ConcurrentLinkedQueue<ElementRef> clearableRefs = new ConcurrentLinkedQueue<>();

  public ReferenceManager(int heapPercentageThreshold) {
    this.heapPercentageThreshold = heapPercentageThreshold;
    installGCMonitoring();
  }

  public void registerRef(ElementRef ref) {
    clearableRefs.add(ref);
  }

  protected void clearReferences() {
    int releaseCount = 20000; //TODO make configurable
    System.out.println("ReferenceManager: clearing " + releaseCount + " references");


    ElementRef ref = clearableRefs.poll();
    while (ref != null && releaseCount > 0) {
      if (ref.isSet()) {
        ref.clear();
        releaseCount--;
      }
      ref = clearableRefs.poll();
    }
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
            System.out.println("heap usage (after GC) is " + heapUsageBefore + ", clearing some references");
            clearReferences();
          }
        }
      };
      NotificationEmitter emitter = (NotificationEmitter) gcbean;
      emitter.addNotificationListener(listener, null, null);
    }
  }

}
