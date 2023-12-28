package org.apache.pulsar.broker.service.persistent;

import java.util.concurrent.atomic.LongAdder;
import lombok.Getter;

@SuppressWarnings("LombokGetterMayBeUsed")
public class PersistentTopicMetrics {

    @Getter
    private final BacklogQuotaMetrics backlogQuotaMetrics = new BacklogQuotaMetrics();

    public static class BacklogQuotaMetrics {
        private final LongAdder timeBasedBacklogQuotaExceededEvictionCount = new LongAdder();
        private final LongAdder sizeBasedBacklogQuotaExceededEvictionCount = new LongAdder();

        public void recordTimeBasedBacklogEviction() {
            timeBasedBacklogQuotaExceededEvictionCount.increment();
        }

        public void recordSizeBasedBacklogEviction() {
            sizeBasedBacklogQuotaExceededEvictionCount.increment();
        }

        public long getSizeBasedBacklogQuotaExceededEvictionCount() {
            return sizeBasedBacklogQuotaExceededEvictionCount.longValue();
        }

        public long getTimeBasedBacklogQuotaExceededEvictionCount() {
            return timeBasedBacklogQuotaExceededEvictionCount.longValue();
        }
    }
}