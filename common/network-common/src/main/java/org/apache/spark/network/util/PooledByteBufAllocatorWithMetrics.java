package org.apache.spark.network.util;

import java.util.Iterator;
import java.util.List;

import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * A {@link PooledByteBufAllocator} providing some metrics.
 */
public class PooledByteBufAllocatorWithMetrics extends PooledByteBufAllocator {

  public PooledByteBufAllocatorWithMetrics(
      boolean preferDirect,
      int nHeapArena,
      int nDirectArena,
      int pageSize,
      int maxOrder,
      int tinyCacheSize,
      int smallCacheSize,
      int normalCacheSize) {
    super(preferDirect, nHeapArena, nDirectArena, pageSize, maxOrder, tinyCacheSize,
      smallCacheSize, normalCacheSize);
  }

  public long offHeapUsage() {
    return sumOfMetrics(directArenas());
  }

  public long onHeapUsage() {
    return sumOfMetrics(heapArenas());
  }

  private long sumOfMetrics(List<PoolArenaMetric> metrics) {
    long sum = 0;
    for (int i = 0; i < metrics.size(); i++) {
      PoolArenaMetric metric = metrics.get(i);
      List<PoolChunkListMetric> chunkListMetrics = metric.chunkLists();
      for (int j = 0; j < chunkListMetrics.size(); j++) {
        PoolChunkListMetric chunkListMetric = chunkListMetrics.get(j);
        Iterator<PoolChunkMetric> chunkMetricIter = chunkListMetric.iterator();
        while (chunkMetricIter.hasNext()) {
          sum += chunkMetricIter.next().chunkSize();
        }
      }
    }
    return sum;
  }
}
