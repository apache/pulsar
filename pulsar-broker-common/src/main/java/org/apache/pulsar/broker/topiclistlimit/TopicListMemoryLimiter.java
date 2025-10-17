/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.topiclistlimit;

import io.netty.buffer.ByteBufUtil;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterImpl;
import org.apache.pulsar.common.semaphore.AsyncSemaphore;

/**
 * Topic list memory limiter that exposes both Prometheus metrics and OpenTelemetry metrics.
 */
@Slf4j
public class TopicListMemoryLimiter extends AsyncDualMemoryLimiterImpl {
    private final CollectorRegistry collectorRegistry;
    private final Gauge heapMemoryUsedBytes;
    private final Gauge heapMemoryLimitBytes;
    private final Gauge directMemoryUsedBytes;
    private final Gauge directMemoryLimitBytes;
    private final Gauge heapQueueSize;
    private final Gauge heapQueueMaxSize;
    private final Gauge directQueueSize;
    private final Gauge directQueueMaxSize;
    private final Summary heapWaitTimeMs;
    private final Summary directWaitTimeMs;
    private final Counter heapTimeoutTotal;
    private final Counter directTimeoutTotal;
    private final ObservableDoubleGauge otelHeapMemoryUsedGauge;
    private final DoubleGauge otelHeapMemoryLimitGauge;
    private final ObservableDoubleGauge otelDirectMemoryUsedGauge;
    private final DoubleGauge otelDirectMemoryLimitGauge;
    private final ObservableLongUpDownCounter otelHeapQueueSize;
    private final ObservableLongUpDownCounter otelDirectQueueSize;
    private final DoubleHistogram otelHeapWaitTime;
    private final DoubleHistogram otelDirectWaitTime;
    private final LongCounter otelHeapTimeoutTotal;
    private final LongCounter otelDirectTimeoutTotal;

    public TopicListMemoryLimiter(CollectorRegistry collectorRegistry, String prometheusPrefix,
                                  Meter openTelemetryMeter,
                                  long maxHeapMemory, int maxHeapQueueSize,
                                  long heapTimeoutMillis, long maxDirectMemory, int maxDirectQueueSize,
                                  long directTimeoutMillis) {
        super(maxHeapMemory, maxHeapQueueSize, heapTimeoutMillis, maxDirectMemory, maxDirectQueueSize,
                directTimeoutMillis);
        this.collectorRegistry = collectorRegistry;

        AsyncSemaphore heapMemoryLimiter = getLimiter(LimitType.HEAP_MEMORY);
        AsyncSemaphore directMemoryLimiter = getLimiter(LimitType.DIRECT_MEMORY);

        this.heapMemoryUsedBytes = register(Gauge.build(prometheusPrefix + "topic_list_heap_memory_used_bytes",
                        "Current heap memory used by topic listings")
                .create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return heapMemoryLimiter.getAcquiredPermits();
                    }
                }));
        this.otelHeapMemoryUsedGauge = openTelemetryMeter.gaugeBuilder("topic.list.heap.memory.used")
                .setUnit("By")
                .setDescription("Current heap memory used by topic listings")
                .buildWithCallback(observableDoubleMeasurement -> {
                    observableDoubleMeasurement.record(heapMemoryLimiter.getAcquiredPermits());
                });

        this.heapMemoryLimitBytes = register(Gauge.build(prometheusPrefix + "topic_list_heap_memory_limit_bytes",
                        "Configured heap memory limit")
                .create());
        this.heapMemoryLimitBytes.set(maxHeapMemory);
        this.otelHeapMemoryLimitGauge = openTelemetryMeter.gaugeBuilder("topic.list.heap.memory.limit")
                .setUnit("By")
                .setDescription("Configured heap memory limit")
                .build();
        this.otelHeapMemoryLimitGauge.set(maxHeapMemory);

        this.directMemoryUsedBytes = register(Gauge.build(prometheusPrefix + "topic_list_direct_memory_used_bytes",
                        "Current direct memory used by topic listings")
                .create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return directMemoryLimiter.getAcquiredPermits();
                    }
                }));
        this.otelDirectMemoryUsedGauge = openTelemetryMeter.gaugeBuilder("topic.list.direct.memory.used")
                .setUnit("By")
                .setDescription("Current direct memory used by topic listings")
                .buildWithCallback(observableDoubleMeasurement -> {
                    observableDoubleMeasurement.record(directMemoryLimiter.getAcquiredPermits());
                });

        this.directMemoryLimitBytes = register(Gauge.build(prometheusPrefix + "topic_list_direct_memory_limit_bytes",
                        "Configured direct memory limit")
                .create());
        this.directMemoryLimitBytes.set(maxDirectMemory);
        this.otelDirectMemoryLimitGauge = openTelemetryMeter.gaugeBuilder("topic.list.direct.memory.limit")
                .setUnit("By")
                .setDescription("Configured direct memory limit")
                .build();
        this.otelDirectMemoryLimitGauge.set(maxDirectMemory);

        this.heapQueueSize = register(Gauge.build(prometheusPrefix + "topic_list_heap_queue_size",
                        "Current heap memory limiter queue size")
                .create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return heapMemoryLimiter.getQueueSize();
                    }
                }));
        this.otelHeapQueueSize = openTelemetryMeter
                .upDownCounterBuilder("topic.list.heap.queue.size")
                .setDescription("Current heap memory limiter queue size")
                .setUnit("1")
                .buildWithCallback(observableLongMeasurement -> {
                    observableLongMeasurement.record(heapMemoryLimiter.getQueueSize());
                });

        this.heapQueueMaxSize = register(Gauge.build(prometheusPrefix + "topic_list_heap_queue_max_size",
                        "Maximum heap memory limiter queue size")
                .create());
        this.heapQueueMaxSize.set(maxHeapQueueSize);
        LongGauge otelHeapQueueMaxSize = openTelemetryMeter
                .gaugeBuilder("topic.list.heap.queue.max.size")
                .setDescription("Maximum heap memory limiter queue size")
                .setUnit("1")
                .ofLongs()
                .build();
        otelHeapQueueMaxSize.set(maxHeapQueueSize);

        this.directQueueSize = register(Gauge.build(prometheusPrefix + "topic_list_direct_queue_size",
                        "Current direct memory limiter queue size")
                .create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return directMemoryLimiter.getQueueSize();
                    }
                }));
        this.otelDirectQueueSize = openTelemetryMeter
                .upDownCounterBuilder("topic.list.direct.queue.size")
                .setDescription("Current direct memory limiter queue size")
                .setUnit("1")
                .buildWithCallback(observableLongMeasurement -> {
                    observableLongMeasurement.record(directMemoryLimiter.getQueueSize());
                });

        this.directQueueMaxSize = register(Gauge.build(prometheusPrefix + "topic_list_direct_queue_max_size",
                        "Maximum direct memory limiter queue size")
                .create());
        this.directQueueMaxSize.set(maxDirectQueueSize);
        LongGauge otelDirectQueueMaxSize = openTelemetryMeter
                .gaugeBuilder("topic.list.direct.queue.max.size")
                .setDescription("Maximum direct memory limiter queue size")
                .setUnit("1")
                .ofLongs()
                .build();
        otelDirectQueueMaxSize.set(maxDirectQueueSize);

        this.heapWaitTimeMs = register(Summary.build(prometheusPrefix + "topic_list_heap_wait_time_ms",
                        "Wait time for heap memory permits")
                .quantile(0.50, 0.01)
                .quantile(0.95, 0.01)
                .quantile(0.99, 0.01)
                .quantile(1, 0.01)
                .create());
        this.otelHeapWaitTime = openTelemetryMeter.histogramBuilder("topic.list.heap.wait.time")
                .setUnit("s")
                .setDescription("Wait time for heap memory permits")
                .build();

        this.directWaitTimeMs = register(Summary.build(prometheusPrefix + "topic_list_direct_wait_time_ms",
                        "Wait time for direct memory permits")
                .quantile(0.50, 0.01)
                .quantile(0.95, 0.01)
                .quantile(0.99, 0.01)
                .quantile(1, 0.01)
                .create());
        this.otelDirectWaitTime = openTelemetryMeter.histogramBuilder("topic.list.direct.wait.time")
                .setUnit("s")
                .setDescription("Wait time for direct memory permits")
                .build();

        this.heapTimeoutTotal = register(Counter.build(prometheusPrefix + "topic_list_heap_timeout_total",
                        "Total heap memory permit timeouts")
                .create());
        this.otelHeapTimeoutTotal = openTelemetryMeter.counterBuilder("topic.list.heap.timeout.total")
                .setDescription("Total heap memory permit timeouts")
                .setUnit("1")
                .build();

        this.directTimeoutTotal = register(Counter.build(prometheusPrefix + "topic_list_direct_timeout_total",
                        "Total direct memory permit timeouts")
                .create());
        this.otelDirectTimeoutTotal = openTelemetryMeter.counterBuilder("topic.list.direct.timeout.total")
                .setDescription("Total direct memory permit timeouts")
                .setUnit("1")
                .build();
    }

    private <T extends Collector> T register(T collector) {
        try {
            collectorRegistry.register(collector);
        } catch (Exception e) {
            // ignore exception when registering a collector that is already registered
            if (log.isDebugEnabled()) {
                log.debug("Failed to register Prometheus collector {}", collector, e);
            }
        }
        return collector;
    }

    private void unregister(Collector collector) {
        try {
            collectorRegistry.unregister(collector);
        } catch (Exception e) {
            // ignore exception when unregistering a collector that is not registered
            if (log.isDebugEnabled()) {
                log.debug("Failed to unregister Prometheus collector {}", collector, e);
            }
        }
    }


    @Override
    protected void recordHeapWaitTime(long waitTimeNanos) {
        if (waitTimeNanos == Long.MAX_VALUE) {
            heapTimeoutTotal.inc();
            otelHeapTimeoutTotal.add(1);
        } else {
            heapWaitTimeMs.observe(TimeUnit.NANOSECONDS.toMillis(waitTimeNanos));
            otelHeapWaitTime.record(waitTimeNanos / 1_000_000_000.0d);
        }
    }

    @Override
    protected void recordDirectWaitTime(long waitTimeNanos) {
        if (waitTimeNanos == Long.MAX_VALUE) {
            directTimeoutTotal.inc();
            otelDirectTimeoutTotal.add(1);
        } else {
            directWaitTimeMs.observe(TimeUnit.NANOSECONDS.toMillis(waitTimeNanos));
            otelDirectWaitTime.record(waitTimeNanos / 1_000_000_000.0d);
        }
    }

    @Override
    public void close() {
        super.close();
        unregister(heapMemoryUsedBytes);
        unregister(heapMemoryLimitBytes);
        unregister(directMemoryUsedBytes);
        unregister(directMemoryLimitBytes);
        unregister(heapQueueSize);
        unregister(heapQueueMaxSize);
        unregister(directQueueSize);
        unregister(directQueueMaxSize);
        unregister(heapWaitTimeMs);
        unregister(directWaitTimeMs);
        unregister(heapTimeoutTotal);
        unregister(directTimeoutTotal);
        otelHeapMemoryUsedGauge.close();
        otelDirectMemoryUsedGauge.close();
        otelHeapQueueSize.close();
        otelDirectQueueSize.close();
    }

    /**
     * Estimate the heap memory size of a topic list.
     * @param topicList the topic list to estimate
     * @return the estimated heap memory size in bytes
     */
    public static long estimateTopicListSize(List<String> topicList) {
        return topicList.stream()
                .mapToLong(ByteBufUtil::utf8Bytes) // convert character count to bytes
                // add 32 bytes overhead for each entry
                // 16 bytes for object header, 16 bytes for java.lang.String fields
                .map(n -> n + 32)
                .sum();
    }
}
