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
package org.apache.pulsar.functions.instance.v5;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionType;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.impl.v5.V5Interop;
import org.apache.pulsar.common.functions.BatchingConfig;
import org.apache.pulsar.common.functions.ProducerConfig;

/**
 * Creates V5 producers for the Functions runtime, configured from a {@link ProducerConfig} the same way
 * {@link org.apache.pulsar.functions.instance.ProducerBuilderFactory} configures v4 producers, and returns them
 * as v4 {@link Producer}s through {@link V5ProducerAdapter}.
 *
 * <p>Settings that have no V5 counterpart are ignored: the pending-message limits (the V5 client is bounded by
 * its memory limit), the batcher type and the round-robin partition switch frequency (a scalable topic routes by
 * key and has no partitions). Producer encryption is not supported yet.
 */
@CustomLog
public class V5ProducerFactory {

    // the batching delay ProducerBuilderFactory uses for v4 producers
    private static final Duration DEFAULT_BATCHING_MAX_PUBLISH_DELAY = Duration.ofMillis(10);

    private final Supplier<PulsarClient> client;
    private final ProducerConfig producerConfig;
    private final CompressionType defaultCompressionType;

    /**
     * @param client supplies the V5 client, which is created on first use
     * @param producerConfig the producer settings, or {@code null} for the defaults
     * @param defaultCompressionType the compression to use when {@code producerConfig} sets none
     */
    public V5ProducerFactory(Supplier<PulsarClient> client, ProducerConfig producerConfig,
                             CompressionType defaultCompressionType) {
        this.client = client;
        this.producerConfig = producerConfig;
        this.defaultCompressionType = defaultCompressionType;
        if (producerConfig != null && producerConfig.getCryptoConfig() != null) {
            throw new UnsupportedOperationException("Producer encryption is not supported with the V5 client yet");
        }
    }

    public <T> Producer<T> createProducer(String topic, Schema<T> schema, String producerName,
                                          Map<String, String> properties) throws PulsarClientException {
        ProducerBuilder<T> builder = client.get().newProducer(V5Interop.toV5Schema(schema))
                .topic(topic)
                .blockIfQueueFull(true)
                // no send timeout, to prevent a deadlock with a consumer that is blocked on unacked messages
                .sendTimeout(Duration.ZERO)
                .compressionPolicy(CompressionPolicy.of(compressionType()))
                .batchingPolicy(batchingPolicy());
        if (producerName != null) {
            builder.producerName(producerName);
        }
        if (properties != null) {
            builder.properties(properties);
        }
        if (producerConfig != null && (producerConfig.getMaxPendingMessages() != null
                || producerConfig.getMaxPendingMessagesAcrossPartitions() != null
                || producerConfig.getBatchBuilder() != null)) {
            log.warn().attr("topic", topic).attr("producerConfig", producerConfig)
                    .log("Ignoring the pending-message limits and the batcher type, which the V5 producer does not"
                            + " have; the V5 client's memory limit bounds the pending messages");
        }
        try {
            return new V5ProducerAdapter<>(builder.create(), schema);
        } catch (org.apache.pulsar.client.api.v5.PulsarClientException e) {
            throw new PulsarClientException(e);
        }
    }

    private CompressionType compressionType() {
        if (producerConfig != null && producerConfig.getCompressionType() != null) {
            return CompressionType.valueOf(producerConfig.getCompressionType().name());
        }
        return defaultCompressionType;
    }

    private BatchingPolicy batchingPolicy() {
        BatchingConfig batchingConfig = producerConfig != null ? producerConfig.getBatchingConfig() : null;
        if (batchingConfig == null) {
            return BatchingPolicy.builder().maxPublishDelay(DEFAULT_BATCHING_MAX_PUBLISH_DELAY).build();
        }
        if (!batchingConfig.isEnabled()) {
            return BatchingPolicy.ofDisabled();
        }
        Integer maxPublishDelayMs = batchingConfig.getBatchingMaxPublishDelayMs();
        BatchingPolicy.Builder builder = BatchingPolicy.builder().maxPublishDelay(
                maxPublishDelayMs != null && maxPublishDelayMs > 0
                        ? Duration.ofMillis(maxPublishDelayMs)
                        : DEFAULT_BATCHING_MAX_PUBLISH_DELAY);
        if (batchingConfig.getBatchingMaxMessages() != null && batchingConfig.getBatchingMaxMessages() > 0) {
            builder.maxMessages(batchingConfig.getBatchingMaxMessages());
        }
        if (batchingConfig.getBatchingMaxBytes() != null && batchingConfig.getBatchingMaxBytes() > 0) {
            builder.maxSize(MemorySize.ofBytes(batchingConfig.getBatchingMaxBytes()));
        }
        return builder.build();
    }
}
