/**
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
package org.apache.kafka.clients.consumer;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.PulsarClientKafkaConfig;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.shade.io.netty.util.concurrent.DefaultThreadFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.TopicFilter;
import kafka.serializer.Decoder;
import lombok.extern.slf4j.Slf4j;

/**
 * It replaces : kafka.javaapi.consumer.ConsumerConnector but not extending kafka-interface because its method has
 * KafkaStream api signature and KafkaStream is a scala class which creates unresolvable dependency conflict src:
 * https://github.com/apache/kafka/blob/0.8.2.2/core/src/main/scala/kafka/javaapi/consumer/ConsumerConnector.java
 */
@Slf4j
public class ConsumerConnector {

    private final PulsarClient client;
    private final boolean isAutoCommit;
    private final ConsumerBuilder<byte[]> consumerBuilder;
    private String clientId;
    private String groupId;
    @SuppressWarnings("rawtypes")
    private final Set<PulsarKafkaStream> topicStreams;
    private SubscriptionInitialPosition strategy = null;
    private final ScheduledExecutorService executor;

    public ConsumerConnector(ConsumerConfig config) {
        checkNotNull(config, "ConsumerConfig can't be null");
        clientId = config.clientId();
        groupId = config.groupId();
        isAutoCommit = config.autoCommitEnable();
        if ("largest".equalsIgnoreCase(config.autoOffsetReset())) {
            strategy = SubscriptionInitialPosition.Latest;
        } else if ("smallest".equalsIgnoreCase(config.autoOffsetReset())) {
            strategy = SubscriptionInitialPosition.Earliest;
        }
        String consumerId = !config.consumerId().isEmpty() ? config.consumerId().get() : null;
        int maxMessage = config.queuedMaxMessages();
        String serviceUrl = config.zkConnect();

        Properties properties = config.props() != null && config.props().props() != null ? config.props().props()
                : new Properties();
        try {
            client = PulsarClientKafkaConfig.getClientBuilder(properties).serviceUrl(serviceUrl).build();
        } catch (PulsarClientException e) {
            throw new IllegalArgumentException(
                    "Failed to create pulsar-client using url = " + serviceUrl + ", properties = " + properties, e);
        }

        topicStreams = Sets.newConcurrentHashSet();
        consumerBuilder = client.newConsumer();
        consumerBuilder.subscriptionName(groupId);
        if (properties.containsKey("queued.max.message.chunks") && config.queuedMaxMessages() > 0) {
            consumerBuilder.receiverQueueSize(maxMessage);
        }
        if (consumerId != null) {
            consumerBuilder.consumerName(consumerId);
        }
        if (properties.containsKey("auto.commit.interval.ms") && config.autoCommitIntervalMs() > 0) {
            consumerBuilder.acknowledgmentGroupTime(config.autoCommitIntervalMs(), TimeUnit.MILLISECONDS);
        }
        this.executor = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("pulsar-kafka"));
    }

    public <K, V> Map<String, List<PulsarKafkaStream<byte[], byte[]>>> createMessageStreams(
            Map<String, Integer> topicCountMap) {
        return createMessageStreamsByFilter(null, topicCountMap, null, null);
    }

    public <K, V> Map<String, List<PulsarKafkaStream<K, V>>> createMessageStreams(Map<String, Integer> topicCountMap,
            Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        return createMessageStreamsByFilter(null, topicCountMap, keyDecoder, valueDecoder);
    }

    public <K, V> Map<String, List<PulsarKafkaStream<K, V>>> createMessageStreamsByFilter(TopicFilter topicFilter,
            Map<String, Integer> topicCountMap, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {

        Map<String, List<PulsarKafkaStream<K, V>>> streams = Maps.newHashMap();

        topicCountMap.forEach((topic, count) -> {
            try {
                Consumer<byte[]> consumer = consumerBuilder.topic(topic).subscribe();
                resetOffsets(consumer, strategy);
                log.info("Creating stream for {}-{} with config {}", topic, groupId, consumerBuilder.toString());
                for (int i = 0; i < count; i++) {
                    PulsarKafkaStream<K, V> stream = new PulsarKafkaStream<>(keyDecoder, valueDecoder, consumer,
                            isAutoCommit, clientId);
                    // if multiple thread-count present then client expects multiple streams reading from the same
                    // topic. so, create multiple stream using the same consumer
                    streams.computeIfAbsent(topic, key -> Lists.newArrayList()).add(stream);
                    topicStreams.add(stream);
                }
            } catch (PulsarClientException e) {
                log.error("Failed to subscribe on topic {} with group-id {}, {}", topic, groupId, e.getMessage(), e);
                throw new RuntimeException("Failed to subscribe on topic " + topic, e);
            }
        });
        return streams;
    }

    public List<PulsarKafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter) {
        throw new UnsupportedOperationException("method not supported");
    }

    public List<PulsarKafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter, int arg1) {
        throw new UnsupportedOperationException("method not supported");
    }

    public <K, V> List<PulsarKafkaStream<K, V>> createMessageStreamsByFilter(TopicFilter topicFilter, int arg1,
            Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        throw new UnsupportedOperationException("method not supported");
    }

    @SuppressWarnings("unchecked")
    public List<CompletableFuture<Void>> commitOffsetsAsync() {
        return topicStreams.stream().map(stream -> (CompletableFuture<Void>) stream.commitOffsets())
                .collect(Collectors.toList());
    }

    /**
     * Commit the offsets of all broker partitions connected by this connector.
     */
    public void commitOffsets() {
        commitOffsetsAsync();
    }

    public void commitOffsets(boolean retryOnFailure) {
        FutureUtil.waitForAll(commitOffsetsAsync()).handle((res, ex) -> {
            if (ex != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to commit offset {}, retrying {}", ex.getMessage(), retryOnFailure);
                }
                if (retryOnFailure) {
                    this.executor.schedule(() -> commitOffsets(retryOnFailure), 30, TimeUnit.SECONDS);
                }
            }
            return null;
        });
    }

    /**
     * Shut down the connector
     */
    public void shutdown() {
        if (executor != null) {
            executor.shutdown();
        }
        if (topicStreams != null) {
            topicStreams.forEach(stream -> {
                try {
                    stream.close();
                } catch (Exception e) {
                    log.warn("Failed to close stream {}, {}", stream, e.getMessage());
                }
            });
        }
        try {
            client.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close client {}", e.getMessage());
        }
    }

    private void resetOffsets(Consumer<byte[]> consumer, SubscriptionInitialPosition strategy) {
        if (strategy == null) {
            return;
        }
        log.info("Resetting partition {} for group-id {} and seeking to {} position", consumer.getTopic(),
                consumer.getSubscription(), strategy);
        try {
            if (strategy == SubscriptionInitialPosition.Earliest) {
                consumer.seek(MessageId.earliest);
            } else {
                consumer.seek(MessageId.latest);
            }
        } catch (PulsarClientException e) {
            log.warn("Failed to reset offset for consumer {} to {}, {}", consumer.getTopic(), strategy,
                    e.getMessage(), e);
        }
    }

    @VisibleForTesting
    public ConsumerBuilder<byte[]> getConsumerBuilder() {
        return consumerBuilder;
    }
}
