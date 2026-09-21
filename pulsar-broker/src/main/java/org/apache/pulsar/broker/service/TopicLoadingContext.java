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
            case "namespace-policies", "local-policies" -> TopicLoadFailureReason.TIMEOUT_LOAD_NAMESPACE_POLICIES;
            case "local-topic-policies", "global-topic-policies" -> TopicLoadFailureReason.TIMEOUT_LOAD_TOPIC_POLICIES;
            case "open-ml" -> TopicLoadFailureReason.TIMEOUT_LOAD_ML;
            case "init", "pre-create-compacted-sub", "replication" -> TopicLoadFailureReason.TIMEOUT_INIT;
            case "deduplication" -> TopicLoadFailureReason.TIMEOUT_DEDUP;
            default -> null;
        };
    }

    private static TopicLoadFailureReason getFailureReason(String pendingStep) {
        return switch (pendingStep) {
            case "namespace-policies", "local-policies" -> TopicLoadFailureReason.FAILED_LOAD_NAMESPACE_POLICIES;
            case "local-topic-policies", "global-topic-policies" -> TopicLoadFailureReason.FAILED_LOAD_TOPIC_POLICIES;
            case "open-ml" -> TopicLoadFailureReason.FAILED_LOAD_ML;
            case "ownership", "2nd-ownership" -> TopicLoadFailureReason.FAILED_CHECK_OWNERSHIP;
            case "topic-exists", "properties" -> TopicLoadFailureReason.FAILED_ACCESS_METADATA_STORE;
            case "init", "pre-create-compacted-sub", "replication", "deduplication" ->
                    TopicLoadFailureReason.FAILED_INIT;
            default -> null;
        };
    }
}
