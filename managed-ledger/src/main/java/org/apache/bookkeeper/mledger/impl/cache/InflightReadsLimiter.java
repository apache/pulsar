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
package org.apache.bookkeeper.mledger.impl.cache;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.prometheus.client.Gauge;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.opentelemetry.Constants;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.InflightReadLimiterUtilization;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;
import org.jctools.queues.SpscArrayQueue;

@Slf4j
public class InflightReadsLimiter implements AutoCloseable {

    public static final String INFLIGHT_READS_LIMITER_LIMIT_METRIC_NAME =
            "pulsar.broker.managed_ledger.inflight.read.limit";
    private final ObservableLongCounter inflightReadsLimitCounter;

    @PulsarDeprecatedMetric(newMetricName = INFLIGHT_READS_LIMITER_LIMIT_METRIC_NAME)
    @Deprecated
    private static final Gauge PULSAR_ML_READS_BUFFER_SIZE = Gauge
            .build()
            .name("pulsar_ml_reads_inflight_bytes")
            .help("Estimated number of bytes retained by data read from storage or cache")
            .register();

    public static final String INFLIGHT_READS_LIMITER_USAGE_METRIC_NAME =
            "pulsar.broker.managed_ledger.inflight.read.usage";
    private final ObservableLongCounter inflightReadsUsageCounter;

    @PulsarDeprecatedMetric(newMetricName = INFLIGHT_READS_LIMITER_USAGE_METRIC_NAME)
    @Deprecated
    private static final Gauge PULSAR_ML_READS_AVAILABLE_BUFFER_SIZE = Gauge
            .build()
            .name("pulsar_ml_reads_available_inflight_bytes")
            .help("Available space for inflight data read from storage or cache")
            .register();

    private final long maxReadsInFlightSize;
    private long remainingBytes;
    private final long acquireTimeoutMillis;
    private final ScheduledExecutorService timeOutExecutor;
    private final boolean enabled;

    record Handle(long permits, long creationTime, boolean success) {
    }

    record QueuedHandle(Handle handle, Consumer<Handle> callback) {
    }

    private final Queue<QueuedHandle> queuedHandles;
    private boolean timeoutCheckRunning = false;

    public InflightReadsLimiter(long maxReadsInFlightSize, int maxReadsInFlightAcquireQueueSize,
                                long acquireTimeoutMillis, ScheduledExecutorService timeOutExecutor,
                                OpenTelemetry openTelemetry) {
        this.maxReadsInFlightSize = maxReadsInFlightSize;
        this.remainingBytes = maxReadsInFlightSize;
        this.acquireTimeoutMillis = acquireTimeoutMillis;
        this.timeOutExecutor = timeOutExecutor;
        if (maxReadsInFlightSize > 0) {
            enabled = true;
            this.queuedHandles = new SpscArrayQueue<>(maxReadsInFlightAcquireQueueSize);
        } else {
            enabled = false;
            this.queuedHandles = null;
            // set it to -1 in order to show in the metrics that the metric is not available
            PULSAR_ML_READS_BUFFER_SIZE.set(-1);
            PULSAR_ML_READS_AVAILABLE_BUFFER_SIZE.set(-1);
        }
        var meter = openTelemetry.getMeter(Constants.BROKER_INSTRUMENTATION_SCOPE_NAME);
        inflightReadsLimitCounter = meter.counterBuilder(INFLIGHT_READS_LIMITER_LIMIT_METRIC_NAME)
                .setDescription("Maximum number of bytes that can be retained by managed ledger data read from storage "
                        + "or cache.")
                .setUnit("By")
                .buildWithCallback(measurement -> {
                    if (!isDisabled()) {
                        measurement.record(maxReadsInFlightSize);
                    }
                });
        inflightReadsUsageCounter = meter.counterBuilder(INFLIGHT_READS_LIMITER_USAGE_METRIC_NAME)
                .setDescription("Estimated number of bytes retained by managed ledger data read from storage or cache.")
                .setUnit("By")
                .buildWithCallback(measurement -> {
                    if (!isDisabled()) {
                        var freeBytes = getRemainingBytes();
                        var usedBytes = maxReadsInFlightSize - freeBytes;
                        measurement.record(freeBytes, InflightReadLimiterUtilization.FREE.attributes);
                        measurement.record(usedBytes, InflightReadLimiterUtilization.USED.attributes);
                    }
                });
    }

    @VisibleForTesting
    public synchronized long getRemainingBytes() {
        return remainingBytes;
    }

    @Override
    public void close() {
        inflightReadsLimitCounter.close();
        inflightReadsUsageCounter.close();
    }

    private static final Handle DISABLED = new Handle(0, 0, true);
    private static final Optional<Handle> DISABLED_OPTIONAL = Optional.of(DISABLED);

    /**
     * Acquires permits from the limiter. If the limiter is disabled, it will immediately return a successful handle.
     * If permits are available, it will return a handle with the acquired permits. If no permits are available,
     * it will return an empty optional and the callback will be called when permits become available or when the
     * acquire timeout is reached. The success field in the handle passed to the callback will be false if the acquire
     * operation times out. The callback should be non-blocking and run on a desired executor handled within the
     * callback itself.
     *
     * A successful handle will have the success field set to true, and the caller must call release with the handle
     * when the permits are no longer needed.
     *
     * If an unsuccessful handle is returned immediately, it means that the queue limit has been reached and the
     * callback will not be called. The caller should fail the read operation in this case to apply backpressure.
     *
     * @param permits  the number of permits to acquire
     * @param callback the callback to be called when the permits are acquired or timed out
     * @return an optional handle that contains the permits if acquired, otherwise an empty optional
     */
    public Optional<Handle> acquire(long permits, Consumer<Handle> callback) {
        if (isDisabled()) {
            return DISABLED_OPTIONAL;
        }
        return internalAcquire(permits, callback);
    }

    private synchronized Optional<Handle> internalAcquire(long permits, Consumer<Handle> callback) {
        Handle handle = new Handle(permits, System.currentTimeMillis(), true);
        if (remainingBytes >= permits) {
            remainingBytes -= permits;
            if (log.isDebugEnabled()) {
                log.debug("acquired permits: {}, creationTime: {}, remainingBytes:{}", permits, handle.creationTime,
                        remainingBytes);
            }
            updateMetrics();
            return Optional.of(handle);
        } else if (permits > maxReadsInFlightSize && remainingBytes == maxReadsInFlightSize) {
            remainingBytes = 0;
            if (log.isInfoEnabled()) {
                log.info("Requested permits {} exceeded maxReadsInFlightSize {}, creationTime: {}, remainingBytes:{}. "
                                + "Allowing request with permits set to maxReadsInFlightSize.",
                        permits, maxReadsInFlightSize, handle.creationTime, remainingBytes);
            }
            updateMetrics();
            return Optional.of(new Handle(maxReadsInFlightSize, handle.creationTime, true));
        } else {
            if (queuedHandles.offer(new QueuedHandle(handle, callback))) {
                scheduleTimeOutCheck(acquireTimeoutMillis);
                return Optional.empty();
            } else {
                log.warn("Failed to queue handle for acquiring permits: {}, creationTime: {}, remainingBytes:{}",
                        permits, handle.creationTime, remainingBytes);
                return Optional.of(new Handle(0, handle.creationTime, false));
            }
        }
    }

    private synchronized void scheduleTimeOutCheck(long delayMillis) {
        if (acquireTimeoutMillis <= 0) {
            return;
        }
        if (!timeoutCheckRunning) {
            timeoutCheckRunning = true;
            timeOutExecutor.schedule(this::timeoutCheck, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    private synchronized void timeoutCheck() {
        timeoutCheckRunning = false;
        long delay = 0;
        while (true) {
            QueuedHandle queuedHandle = queuedHandles.peek();
            if (queuedHandle != null) {
                long age = System.currentTimeMillis() - queuedHandle.handle.creationTime;
                if (age >= acquireTimeoutMillis) {
                    // remove the peeked handle from the queue
                    queuedHandles.poll();
                    handleTimeout(queuedHandle);
                } else {
                    delay = acquireTimeoutMillis - age;
                    break;
                }
            } else {
                break;
            }
        }
        if (delay > 0) {
            scheduleTimeOutCheck(delay);
        }
    }

    private void handleTimeout(QueuedHandle queuedHandle) {
        if (log.isDebugEnabled()) {
            log.debug("timed out queued permits: {}, creationTime: {}, remainingBytes:{}",
                    queuedHandle.handle.permits, queuedHandle.handle.creationTime, remainingBytes);
        }
        try {
            queuedHandle.callback.accept(new Handle(0, queuedHandle.handle.creationTime, false));
        } catch (Exception e) {
            log.error("Error in callback of timed out queued permits: {}, creationTime: {}, remainingBytes:{}",
                    queuedHandle.handle.permits, queuedHandle.handle.creationTime, remainingBytes, e);
        }
    }

    /**
     * Releases permits back to the limiter. If the handle is disabled, this method will be a no-op.
     *
     * @param handle the handle containing the permits to release
     */
    public void release(Handle handle) {
        if (handle == DISABLED) {
            return;
        }
        internalRelease(handle);
    }

    private synchronized void internalRelease(Handle handle) {
        if (log.isDebugEnabled()) {
            log.debug("release permits: {}, creationTime: {}, remainingBytes:{}", handle.permits,
                    handle.creationTime, getRemainingBytes());
        }
        remainingBytes += handle.permits;
        while (true) {
            QueuedHandle queuedHandle = queuedHandles.peek();
            if (queuedHandle != null) {
                boolean timedOut = acquireTimeoutMillis > 0
                        && System.currentTimeMillis() - queuedHandle.handle.creationTime > acquireTimeoutMillis;
                if (timedOut) {
                    // remove the peeked handle from the queue
                    queuedHandles.poll();
                    handleTimeout(queuedHandle);
                } else if (remainingBytes >= queuedHandle.handle.permits
                        || queuedHandle.handle.permits > maxReadsInFlightSize
                        && remainingBytes == maxReadsInFlightSize) {
                    // remove the peeked handle from the queue
                    queuedHandles.poll();
                    handleQueuedHandle(queuedHandle);
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        updateMetrics();
    }

    private void handleQueuedHandle(QueuedHandle queuedHandle) {
        long permits = queuedHandle.handle.permits;
        Handle handleForCallback = queuedHandle.handle;
        if (permits > maxReadsInFlightSize && remainingBytes == maxReadsInFlightSize) {
            remainingBytes = 0;
            if (log.isInfoEnabled()) {
                log.info("Requested permits {} exceeded maxReadsInFlightSize {}, creationTime: {}, remainingBytes:{}. "
                                + "Allowing request with permits set to maxReadsInFlightSize.",
                        permits, maxReadsInFlightSize, queuedHandle.handle.creationTime, remainingBytes);
            }
            handleForCallback = new Handle(maxReadsInFlightSize, queuedHandle.handle.creationTime, true);
        } else {
            remainingBytes -= permits;
            if (log.isDebugEnabled()) {
                log.debug("acquired queued permits: {}, creationTime: {}, remainingBytes:{}",
                        permits, queuedHandle.handle.creationTime, remainingBytes);
            }
        }
        try {
            queuedHandle.callback.accept(handleForCallback);
        } catch (Exception e) {
            log.error("Error in callback of acquired queued permits: {}, creationTime: {}, remainingBytes:{}",
                    handleForCallback.permits, handleForCallback.creationTime, remainingBytes, e);
        }
    }

    private synchronized void updateMetrics() {
        PULSAR_ML_READS_BUFFER_SIZE.set(maxReadsInFlightSize - remainingBytes);
        PULSAR_ML_READS_AVAILABLE_BUFFER_SIZE.set(remainingBytes);
    }

    public boolean isDisabled() {
        return !enabled;
    }
}