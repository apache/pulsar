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
package org.apache.pulsar.broker.stats;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class OpenTelemetryTopicStats implements AutoCloseable {

    private static final String INSTRUMENT_PREFIX = "pulsar.broker.messaging.topic.";


    private final ObservableLongMeasurement subscriptionCounter;
    private final ObservableLongMeasurement producerCounter;
    private final ObservableLongMeasurement consumerCounter;
    private final ObservableLongMeasurement messageIncomingCounter;
    private final ObservableLongMeasurement messageOutgoingCounter;
    private final ObservableLongMeasurement publishRateLimitCounter;
    private final ObservableLongMeasurement bytesIncomingCounter;
    private final ObservableLongMeasurement bytesOutgoingCounter;
    private final ObservableLongMeasurement storageCounter;

    private final BatchCallback batchCallback;

    private final GetStatsOptions getStatsOptions;

    public OpenTelemetryTopicStats(PulsarService pulsar) {
        var meter = pulsar.getOpenTelemetry().getMeter();

        subscriptionCounter = meter
                .upDownCounterBuilder(INSTRUMENT_PREFIX + "subscription")
                .setUnit("{subscription}")
                .buildObserver();

        producerCounter = meter
                .upDownCounterBuilder(INSTRUMENT_PREFIX + "producer")
                .setUnit("{producer}")
                .buildObserver();

        consumerCounter = meter
                .upDownCounterBuilder(INSTRUMENT_PREFIX + "consumer")
                .setUnit("{consumer}")
                .buildObserver();

        messageIncomingCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "message.incoming")
                .setUnit("{message}")
                .buildObserver();

        messageOutgoingCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "message.outgoing")
                .setUnit("{message}")
                .buildObserver();

        publishRateLimitCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "publish.rate.limit.exceeded")
                .setUnit("{operation}")
                .buildObserver();

        bytesIncomingCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "byte.incoming")
                .setUnit("{byte}")
                .buildObserver();

        bytesOutgoingCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "byte.outgoing")
                .setUnit("{byte}")
                .buildObserver();

        storageCounter = meter.counterBuilder(INSTRUMENT_PREFIX + "storage")
                .setUnit("{byte}")
                .buildObserver();

        batchCallback = meter.batchCallback(() -> pulsar.getBrokerService()
                        .getTopics()
                        .values()
                        .stream()
                        .map(topicFuture -> topicFuture.getNow(Optional.empty()))
                        .forEach(topic -> topic.ifPresent(this::recordMetricsForTopic)),
                subscriptionCounter,
                producerCounter,
                consumerCounter,
                messageIncomingCounter,
                messageOutgoingCounter,
                publishRateLimitCounter);

        getStatsOptions = GetStatsOptions.builder()
                .getPreciseBacklog(true)
                .subscriptionBacklogSize(true)
                .getEarliestTimeInBacklog(true)
                .excludePublishers(false)
                .excludeConsumers(false)
                .build();
    }

    @Override
    public void close() {
        batchCallback.close();
    }

    private void recordMetricsForTopic(Topic topic) {
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic.getName())
                .build();
        subscriptionCounter.record(100, attributes);

        var topicStatsImpl = topic.getStats(getStatsOptions);

        storageCounter.record(100, Attributes.builder()
                .putAll(attributes)
                .put(OpenTelemetryAttributes.PULSAR_STORAGE_TYPE, "logical")
                .build());

        storageCounter.record(100, Attributes.builder()
                .putAll(attributes)
                .put(OpenTelemetryAttributes.PULSAR_STORAGE_TYPE, "backlog")
                .build());

        storageCounter.record(100, Attributes.builder()
                .putAll(attributes)
                .put(OpenTelemetryAttributes.PULSAR_STORAGE_TYPE, "offloaded")
                .build());

        subscriptionCounter.record(topicStatsImpl.getSubscriptions().size(), attributes);
        producerCounter.record(topicStatsImpl.getPublishers().size(), attributes);
        messageIncomingCounter.record(topicStatsImpl.msgInCounter, attributes);
        messageOutgoingCounter.record(topicStatsImpl.msgOutCounter, attributes);
    }

}
