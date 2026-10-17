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
package org.apache.pulsar.broker.service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicMigratedException;
import org.apache.pulsar.broker.stats.BrokerOperabilityMetrics.TopicLoadFailureReason;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.LatencyTracer;
import org.apache.pulsar.common.util.LatencyTracer.TracePoint;
import org.jspecify.annotations.Nullable;

public class TopicLoadingContext extends LatencyTracer {

    @Getter
    private final TopicName topicName;
    @Getter
    private final boolean createIfMissing;
    @Getter
    private final CompletableFuture<Optional<Topic>> topicFuture;
    private final PulsarStats pulsarStats;
    @Nullable
    private volatile Long timeoutTimeInMillis;
    @Getter
    @Setter
    @Nullable private Map<String, String> properties;

    public TopicLoadingContext(TopicName topicName, boolean createIfMissing,
                               CompletableFuture<Optional<Topic>> topicFuture, PulsarStats pulsarStats) {
        super(System::nanoTime, 32);
        this.topicName = topicName;
        this.createIfMissing = createIfMissing;
        this.topicFuture = topicFuture;
        this.pulsarStats = pulsarStats;
    }

    public void close(boolean timedOut) {
        if (timedOut) {
            this.timeoutTimeInMillis = System.currentTimeMillis();
        }
        super.close();
    }

    @Override
    @Nullable
    public Long getTimeoutTimeInMillis() {
        return timeoutTimeInMillis;
    }

    public void recordTopicLoadFailureMetric(Throwable throwable) {
        if (throwable instanceof TopicMigratedException) {
            return;
        }
        if (throwable instanceof TimeoutException) {
            pulsarStats.recordTopicLoadFailed(getTopicLoadTimeoutReason());
        } else if (throwable instanceof ServiceUnitNotReadyException) {
            pulsarStats.recordTopicLoadFailed(TopicLoadFailureReason.BUNDLE_UNLOADING);
        } else {
            TopicLoadFailureReason reason = getTopicLoadFailureReason();
            pulsarStats.recordTopicLoadFailed(reason != null ? reason : TopicLoadFailureReason.OTHERS);
        }
    }

    @Override
    protected String resolveFailureReason(TracePoint tracePoint) {
        Throwable throwable = getTracePointFailure(tracePoint);
        TopicLoadFailureReason reason = throwable instanceof TimeoutException
                ? getTimeoutReason(tracePoint.name()) : getFailureReason(tracePoint.name());
        return reason == null ? super.resolveFailureReason(tracePoint) : reason.name();
    }

    public TopicLoadFailureReason getTopicLoadFailureReason() {
        String reason = getFailureReason();
        try {
            return reason == null ? null : TopicLoadFailureReason.valueOf(reason);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public TopicLoadFailureReason getTopicLoadTimeoutReason() {
        // If closed, we can use the reverse convenience set to obtain the last pending action, as the subsequent ones
        // are often sub-actions of the previous one.
        if (isClosed()) {
            for (int i = tracePoints.size() - 1; i >= 0; i--) {
                if (tracePoints.get(i).isPending()) {
                    TopicLoadFailureReason reason = getTimeoutReason(tracePoints.get(i).name());
                    if (reason != null) {
                        return reason;
                    }
                }
            }
        }
        for (TracePoint pendingTracePoint : getPendingTracePoints()) {
            TopicLoadFailureReason reason = getTimeoutReason(pendingTracePoint.name());
            if (reason != null) {
                return reason;
            }
        }
        return TopicLoadFailureReason.TIMEOUT;
    }

    private static TopicLoadFailureReason getTimeoutReason(String pendingStep) {
        return switch (pendingStep) {
            case TopicLoadingTracePoints.NAMESPACE_POLICIES, TopicLoadingTracePoints.LOCAL_POLICIES ->
                    TopicLoadFailureReason.TIMEOUT_LOAD_NAMESPACE_POLICIES;
            case TopicLoadingTracePoints.LOCAL_TOPIC_POLICIES, TopicLoadingTracePoints.GLOBAL_TOPIC_POLICIES ->
                    TopicLoadFailureReason.TIMEOUT_LOAD_TOPIC_POLICIES;
            case TopicLoadingTracePoints.OPEN_ML -> TopicLoadFailureReason.TIMEOUT_LOAD_ML;
            case TopicLoadingTracePoints.INIT, TopicLoadingTracePoints.PRE_CREATE_COMPACTED_SUB,
                    TopicLoadingTracePoints.REPLICATION -> TopicLoadFailureReason.TIMEOUT_INIT;
            case TopicLoadingTracePoints.DEDUPLICATION -> TopicLoadFailureReason.TIMEOUT_DEDUP;
            default -> null;
        };
    }

    private static TopicLoadFailureReason getFailureReason(String pendingStep) {
        return switch (pendingStep) {
            case TopicLoadingTracePoints.NAMESPACE_POLICIES, TopicLoadingTracePoints.LOCAL_POLICIES ->
                    TopicLoadFailureReason.FAILED_LOAD_NAMESPACE_POLICIES;
            case TopicLoadingTracePoints.LOCAL_TOPIC_POLICIES, TopicLoadingTracePoints.GLOBAL_TOPIC_POLICIES ->
                    TopicLoadFailureReason.FAILED_LOAD_TOPIC_POLICIES;
            case TopicLoadingTracePoints.OPEN_ML -> TopicLoadFailureReason.FAILED_LOAD_ML;
            case TopicLoadingTracePoints.OWNERSHIP -> TopicLoadFailureReason.FAILED_CHECK_OWNERSHIP;
            case TopicLoadingTracePoints.TOPIC_EXISTS, TopicLoadingTracePoints.PROPERTIES ->
                    TopicLoadFailureReason.FAILED_ACCESS_METADATA_STORE;
            case TopicLoadingTracePoints.INIT, TopicLoadingTracePoints.PRE_CREATE_COMPACTED_SUB,
                    TopicLoadingTracePoints.REPLICATION, TopicLoadingTracePoints.DEDUPLICATION ->
                    TopicLoadFailureReason.FAILED_INIT;
            default -> null;
        };
    }
}
