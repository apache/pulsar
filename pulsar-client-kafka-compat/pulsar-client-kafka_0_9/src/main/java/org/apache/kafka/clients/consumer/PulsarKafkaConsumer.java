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

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.kafka.compat.PulsarClientKafkaConfig;
import org.apache.pulsar.client.kafka.compat.PulsarConsumerKafkaConfig;
import org.apache.pulsar.client.kafka.compat.PulsarKafkaSchema;
import org.apache.pulsar.client.util.ConsumerName;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class PulsarKafkaConsumer<K, V> implements Consumer<K, V>, MessageListener<byte[]> {

    private static final long serialVersionUID = 1L;

    private final PulsarClient client;

    private final Schema<K> keySchema;
    private final Schema<V> valueSchema;

    private final String groupId;
    private final boolean isAutoCommit;

    private final ConcurrentMap<TopicPartition, org.apache.pulsar.client.api.Consumer<byte[]>> consumers = new ConcurrentHashMap<>();

    private final Map<TopicPartition, Long> lastReceivedOffset = new ConcurrentHashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> lastCommittedOffset = new ConcurrentHashMap<>();
    private final Set<TopicPartition> unpolledPartitions = new HashSet<>();
    private final SubscriptionInitialPosition strategy;

    private volatile boolean closed = false;

    private final int maxRecordsInSinglePoll;

    private final Properties properties;


    private static class QueueItem {
        final org.apache.pulsar.client.api.Consumer<byte[]> consumer;
        final Message<byte[]> message;

        QueueItem(org.apache.pulsar.client.api.Consumer<byte[]> consumer, Message<byte[]> message) {
            this.consumer = consumer;
            this.message = message;
        }
    }

    // Since a single Kafka consumer can receive from multiple topics, we need to multiplex all the different
    // topics/partitions into a single queues
    private final BlockingQueue<QueueItem> receivedMessages = new ArrayBlockingQueue<>(1000);

    public PulsarKafkaConsumer(Map<String, Object> configs) {
        this(new ConsumerConfig(configs), null, null);
    }

    public PulsarKafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
                               Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(configs),
                new PulsarKafkaSchema<>(keyDeserializer), new PulsarKafkaSchema<>(valueDeserializer));
    }

    public PulsarKafkaConsumer(Map<String, Object> configs, Schema<K> keySchema, Schema<V> valueSchema) {
        this(new ConsumerConfig(configs), keySchema, valueSchema);
    }

    public PulsarKafkaConsumer(Properties properties) {
        this(new ConsumerConfig(properties), null, null);
    }

    public PulsarKafkaConsumer(Properties properties, Deserializer<K> keyDeserializer,
                               Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(properties),
                new PulsarKafkaSchema<>(keyDeserializer), new PulsarKafkaSchema<>(valueDeserializer));
    }

    public PulsarKafkaConsumer(Properties properties, Schema<K> keySchema, Schema<V> valueSchema) {
        this(new ConsumerConfig(properties), keySchema, valueSchema);
    }

    @SuppressWarnings("unchecked")
    private PulsarKafkaConsumer(ConsumerConfig consumerConfig, Schema<K> keySchema, Schema<V> valueSchema) {

        if (keySchema == null) {
            Deserializer<K> kafkaKeyDeserializer = consumerConfig.getConfiguredInstance(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            kafkaKeyDeserializer.configure(consumerConfig.originals(), true);
            this.keySchema = new PulsarKafkaSchema<>(kafkaKeyDeserializer);
        } else {
            this.keySchema = keySchema;
            consumerConfig.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        }

        if (valueSchema == null) {
            Deserializer<V> kafkaValueDeserializer = consumerConfig.getConfiguredInstance(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
            kafkaValueDeserializer.configure(consumerConfig.originals(), true);
            this.valueSchema = new PulsarKafkaSchema<>(kafkaValueDeserializer);
        } else {
            this.valueSchema = valueSchema;
            consumerConfig.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        }

        groupId = consumerConfig.getString(ConsumerConfig.GROUP_ID_CONFIG);
        isAutoCommit = consumerConfig.getBoolean(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        strategy = getStrategy(consumerConfig.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        log.info("Offset reset strategy has been assigned value {}", strategy);

        String serviceUrl = consumerConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).get(0);

        // there is not this config in kafka 0.9, so use default value.
        maxRecordsInSinglePoll = 1000;

        this.properties = new Properties();
        consumerConfig.originals().forEach(properties::put);
        ClientBuilder clientBuilder = PulsarClientKafkaConfig.getClientBuilder(properties);
        // Since this client instance is going to be used just for the consumers, we can enable Nagle to group
        // all the acknowledgments sent to broker within a short time frame
        clientBuilder.enableTcpNoDelay(false);
        try {
            client = clientBuilder.serviceUrl(serviceUrl).build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private SubscriptionInitialPosition getStrategy(final String strategy) {
        if ("earliest".equals(strategy)) {
            return SubscriptionInitialPosition.Earliest;
        } else {
            return SubscriptionInitialPosition.Latest;
        }
    }

    @Override
    public Set<TopicPartition> assignment() {
        throw new UnsupportedOperationException("Cannot access the partitions assignements");
    }

    @Override
    public Set<String> subscription() {
        return consumers.keySet().stream().map(TopicPartition::topic).collect(Collectors.toSet());
    }

    @Override
    public void subscribe(List<String> topics) {
        subscribe(topics, null);
    }

    @Override
    public void subscribe(List<String> topics, ConsumerRebalanceListener callback) {
        List<CompletableFuture<org.apache.pulsar.client.api.Consumer<byte[]>>> futures = new ArrayList<>();

        List<TopicPartition> topicPartitions = new ArrayList<>();
        try {
            for (String topic : topics) {
                // Create individual subscription on each partition, that way we can keep using the
                // acknowledgeCumulative()
                int numberOfPartitions = ((PulsarClientImpl) client).getNumberOfPartitions(topic).get();

                ConsumerBuilder<byte[]> consumerBuilder = PulsarConsumerKafkaConfig.getConsumerBuilder(client, properties);
                consumerBuilder.subscriptionType(SubscriptionType.Failover);
                consumerBuilder.messageListener(this);
                consumerBuilder.subscriptionName(groupId);
                if (numberOfPartitions > 1) {
                    // Subscribe to each partition
                    consumerBuilder.consumerName(ConsumerName.generateRandomName());
                    for (int i = 0; i < numberOfPartitions; i++) {
                        String partitionName = TopicName.get(topic).getPartition(i).toString();
                        CompletableFuture<org.apache.pulsar.client.api.Consumer<byte[]>> future = consumerBuilder.clone()
                                .topic(partitionName).subscribeAsync();
                        int partitionIndex = i;
                        TopicPartition tp = new TopicPartition(
                                TopicName.get(topic).getPartitionedTopicName(),
                                partitionIndex);
                        futures.add(future.thenApply(consumer -> {
                            log.info("Add consumer {} for partition {}", consumer, tp);
                            consumers.putIfAbsent(tp, consumer);
                            return consumer;
                        }));
                        topicPartitions.add(tp);
                    }
                } else {
                    // Topic has a single partition
                    CompletableFuture<org.apache.pulsar.client.api.Consumer<byte[]>> future = consumerBuilder.topic(topic)
                            .subscribeAsync();
                    TopicPartition tp = new TopicPartition(
                            TopicName.get(topic).getPartitionedTopicName(),
                            0);
                    futures.add(future.thenApply(consumer -> {
                        log.info("Add consumer {} for partition {}", consumer, tp);
                        consumers.putIfAbsent(tp, consumer);
                        return consumer;
                    }));
                    topicPartitions.add(tp);
                }
            }
            unpolledPartitions.addAll(topicPartitions);

            // Wait for all consumers to be ready
            futures.forEach(CompletableFuture::join);

            // Notify the listener is now owning all topics/partitions
            if (callback != null) {
                callback.onPartitionsAssigned(topicPartitions);
            }

        } catch (Exception e) {
            // Close all consumer that might have been successfully created
            futures.forEach(f -> {
                try {
                    f.get().close();
                } catch (Exception e1) {
                    // Ignore. Consumer already had failed
                }
            });

            throw new RuntimeException(e);
        }
    }

    @Override
    public void assign(List<TopicPartition> list) {
        throw new UnsupportedOperationException("Cannot manually assign partitions");
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener consumerRebalanceListener) {
        throw new UnsupportedOperationException("Cannot subscribe with topic name pattern");
    }

    @Override
    public void unsubscribe() {
        consumers.values().forEach(c -> {
            try {
                c.unsubscribe();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeoutMillis) {
        try {
            QueueItem item = receivedMessages.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            if (item == null) {
                return (ConsumerRecords<K, V>) ConsumerRecords.EMPTY;
            }

            Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();

            int numberOfRecords = 0;

            while (item != null) {
                TopicName topicName = TopicName.get(item.consumer.getTopic());
                String topic = topicName.getPartitionedTopicName();
                int partition = topicName.isPartitioned() ? topicName.getPartitionIndex() : 0;
                Message<byte[]> msg = item.message;
                MessageIdImpl msgId = (MessageIdImpl) msg.getMessageId();
                long offset = MessageIdUtils.getOffset(msgId);

                TopicPartition tp = new TopicPartition(topic, partition);
                if (lastReceivedOffset.get(tp) == null && !unpolledPartitions.contains(tp)) {
                    log.info("When polling offsets, invalid offsets were detected. Resetting topic partition {}", tp);
                    resetOffsets(tp);
                }

                K key = getKey(topic, msg);
                if (valueSchema instanceof PulsarKafkaSchema) {
                    ((PulsarKafkaSchema<V>) valueSchema).setTopic(topic);
                }
                V value = valueSchema.decode(msg.getData());

                ConsumerRecord<K, V> consumerRecord = new ConsumerRecord<>(topic, partition, offset, key, value);

                records.computeIfAbsent(tp, k -> new ArrayList<>()).add(consumerRecord);

                // Update last offset seen by application
                lastReceivedOffset.put(tp, offset);
                unpolledPartitions.remove(tp);

                if (++numberOfRecords >= maxRecordsInSinglePoll) {
                    break;
                }

                // Check if we have an item already available
                item = receivedMessages.poll(0, TimeUnit.MILLISECONDS);
            }

            if (isAutoCommit && !records.isEmpty()) {
                // Commit the offset of previously dequeued messages
                commitAsync();
            }

            return new ConsumerRecords<>(records);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitSync() {
        try {
            doCommitOffsets(getCurrentOffsetsMap()).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            doCommitOffsets(offsets).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitAsync() {
        doCommitOffsets(getCurrentOffsetsMap());
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        Map<TopicPartition, OffsetAndMetadata> offsets = getCurrentOffsetsMap();
        doCommitOffsets(offsets).handle((v, throwable) -> {
            callback.onComplete(offsets, throwable != null ? new Exception(throwable) : null);
            return null;
        });
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        doCommitOffsets(offsets).handle((v, throwable) -> {
            callback.onComplete(offsets, throwable != null ? new Exception(throwable) : null);
            return null;
        });
    }

    private CompletableFuture<Void> doCommitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        offsets.forEach((topicPartition, offsetAndMetadata) -> {
            org.apache.pulsar.client.api.Consumer<byte[]> consumer = consumers.get(topicPartition);
            lastCommittedOffset.put(topicPartition, offsetAndMetadata);
            futures.add(consumer.acknowledgeCumulativeAsync(MessageIdUtils.getMessageId(offsetAndMetadata.offset())));
        });

        return FutureUtil.waitForAll(futures);
    }

    private Map<TopicPartition, OffsetAndMetadata> getCurrentOffsetsMap() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        lastReceivedOffset.forEach((topicPartition, offset) -> {
            OffsetAndMetadata om = new OffsetAndMetadata(offset);
            offsets.put(topicPartition, om);
        });

        return offsets;
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        MessageId msgId = MessageIdUtils.getMessageId(offset);
        org.apache.pulsar.client.api.Consumer<byte[]> c = consumers.get(partition);
        if (c == null) {
            throw new IllegalArgumentException("Cannot seek on a partition where we are not subscribed");
        }

        try {
            c.seek(msgId);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void seekToBeginning(TopicPartition... partitions) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        if (partitions.length == 0) {
            partitions = consumers.keySet().toArray(new TopicPartition[0]);
        }
        lastCommittedOffset.clear();
        lastReceivedOffset.clear();

        for (TopicPartition tp : partitions) {
            org.apache.pulsar.client.api.Consumer<byte[]> c = consumers.get(tp);
            if (c == null) {
                futures.add(FutureUtil.failedFuture(
                        new IllegalArgumentException("Cannot seek on a partition where we are not subscribed")));
            } else {
                futures.add(c.seekAsync(MessageId.earliest));
            }
        }

        FutureUtil.waitForAll(futures).join();
    }

    @Override
    public void seekToEnd(TopicPartition... partitions) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        if (partitions.length == 0) {
            partitions = consumers.keySet().toArray(new TopicPartition[0]);
        }
        lastCommittedOffset.clear();
        lastReceivedOffset.clear();

        for (TopicPartition tp : partitions) {
            org.apache.pulsar.client.api.Consumer<byte[]> c = consumers.get(tp);
            if (c == null) {
                futures.add(FutureUtil.failedFuture(
                        new IllegalArgumentException("Cannot seek on a partition where we are not subscribed")));
            } else {
                futures.add(c.seekAsync(MessageId.latest));
            }
        }

        FutureUtil.waitForAll(futures).join();
    }

    @Override
    public long position(TopicPartition partition) {
        Long offset = lastReceivedOffset.get(partition);
        if (offset == null && !unpolledPartitions.contains(partition)) {
            return resetOffsets(partition).getValue();
        }
        return unpolledPartitions.contains(partition) ? 0 : offset;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return lastCommittedOffset.get(partition);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pause(TopicPartition... topicPartitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume(TopicPartition... topicPartitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            closed = true;

            if (isAutoCommit) {
                commitAsync();
            }

            client.closeAsync().get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void wakeup() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method is called whenever a new message is received.
     * <p>
     * Messages are guaranteed to be delivered in order and from the same thread for a single consumer
     * <p>
     * This method will only be called once for each message, unless either application or broker crashes.
     * <p>
     * Application is responsible of handling any exception that could be thrown while processing the message.
     *
     * @param consumer the consumer that received the message
     * @param msg
     */
    @Override
    public void received(org.apache.pulsar.client.api.Consumer<byte[]> consumer, Message<byte[]> msg) {
        // Block listener thread if the application is slowing down
        try {
            receivedMessages.put(new QueueItem(consumer, msg));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (closed) {
                // Consumer was closed and the thread was interrupted. Nothing to worry about here
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private SubscriptionInitialPosition resetOffsets(final TopicPartition partition) {
        log.info("Resetting partition {} and seeking to {} position", partition, strategy);
        if (strategy == SubscriptionInitialPosition.Earliest) {
            seekToBeginning(partition);
        } else {
            seekToEnd(partition);
        }
        return strategy;
    }

    @SuppressWarnings("unchecked")
    private K getKey(String topic, Message<byte[]> msg) {
        if (!msg.hasKey()) {
            return null;
        }

        if (keySchema instanceof PulsarKafkaSchema) {
            PulsarKafkaSchema<K> pulsarKafkaSchema = (PulsarKafkaSchema) keySchema;
            Deserializer<K> kafkaDeserializer = pulsarKafkaSchema.getKafkaDeserializer();
            if (kafkaDeserializer instanceof StringDeserializer) {
                return (K) msg.getKey();
            }
            pulsarKafkaSchema.setTopic(topic);
        }
        // Assume base64 encoding
        byte[] data = Base64.getDecoder().decode(msg.getKey());
        return keySchema.decode(data);

    }
}
