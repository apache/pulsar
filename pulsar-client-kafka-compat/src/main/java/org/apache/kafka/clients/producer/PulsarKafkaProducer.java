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
package org.apache.kafka.clients.producer;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.kafka.compat.PulsarKafkaConfig;

public class PulsarKafkaProducer<K, V> implements Producer<K, V> {

    private final PulsarClient client;
    private final ProducerConfiguration pulsarProducerConf;

    private final ConcurrentMap<String, org.apache.pulsar.client.api.Producer> producers = new ConcurrentHashMap<>();

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    private final AtomicLong outstandingWrites = new AtomicLong();

    public PulsarKafkaProducer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    public PulsarKafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        this(configs, new Properties(), keySerializer, valueSerializer);
    }

    public PulsarKafkaProducer(Properties properties) {
        this(properties, null, null);
    }

    public PulsarKafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new HashMap<>(), properties, keySerializer, valueSerializer);
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    private PulsarKafkaProducer(Map<String, Object> conf, Properties properties, Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        properties.forEach((k, v) -> conf.put((String) k, v));

        ProducerConfig producerConfig = new ProducerConfig(conf);

        if (keySerializer == null) {
            this.keySerializer = producerConfig.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    Serializer.class);
            this.keySerializer.configure(producerConfig.originals(), true);
        } else {
            this.keySerializer = keySerializer;
            producerConfig.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        }

        if (valueSerializer == null) {
            this.valueSerializer = producerConfig.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    Serializer.class);
            this.valueSerializer.configure(producerConfig.originals(), true);
        } else {
            this.valueSerializer = valueSerializer;
            producerConfig.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }

        String serviceUrl = producerConfig.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).get(0);
        ClientConfiguration clientConf = PulsarKafkaConfig.getClientConfiguration(properties);
        try {
            client = PulsarClient.create(serviceUrl, clientConf);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        pulsarProducerConf = new ProducerConfiguration();
        pulsarProducerConf.setBatchingEnabled(true);
        pulsarProducerConf.setBatchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);

        pulsarProducerConf.setBlockIfQueueFull(
                Boolean.parseBoolean(properties.getProperty(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false")));
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        org.apache.pulsar.client.api.Producer producer = producers.computeIfAbsent(record.topic(),
                topic -> createNewProducer(topic));

        Message msg = getMessage(record);

        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();

        outstandingWrites.incrementAndGet();

        producer.sendAsync(msg).thenAccept((messageId) -> {
            decreaseOutstandingWrites();
            future.complete(getRecordMetadata(record.topic(), msg, messageId));
        }).exceptionally(ex -> {
            decreaseOutstandingWrites();
            future.completeExceptionally(ex);
            return null;
        });

        return future;
    }

    private void decreaseOutstandingWrites() {
        synchronized (outstandingWrites) {
            if (outstandingWrites.decrementAndGet() == 0L) {
                outstandingWrites.notifyAll();
            }
        }
    }

    @Override
    public void flush() {
        synchronized (outstandingWrites) {
            while (outstandingWrites.get() != 0) {
                try {
                    outstandingWrites.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return Collections.emptyList();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.emptyMap();
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        try {
            client.closeAsync().get(timeout, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private org.apache.pulsar.client.api.Producer createNewProducer(String topic) {
        try {
            return client.createProducer(topic);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private Message getMessage(ProducerRecord<K, V> record) {
        MessageBuilder builder = MessageBuilder.create();
        if (record.key() != null) {
            builder.setKey(getKey(record.topic(), record.key()));
        }
        builder.setContent(valueSerializer.serialize(record.topic(), record.value()));
        return builder.build();
    }

    private String getKey(String topic, K key) {
        // If key is a String, we can use it as it is, otherwise, serialize to byte[] and encode in base64
        if (keySerializer instanceof StringSerializer) {
            return (String) key;
        } else {
            byte[] keyBytes = keySerializer.serialize(topic, key);
            return Base64.getEncoder().encodeToString(keyBytes);
        }
    }

    private RecordMetadata getRecordMetadata(String topic, Message msg, MessageId messageId) {
        MessageIdImpl msgId = (MessageIdImpl) messageId;
        long ledgerId = msgId.getLedgerId();
        long entryId = msgId.getEntryId();

        // Combine ledger id and entry id to form offset
        long offset = (ledgerId << 36) & entryId;

        int partition = msgId.getPartitionIndex();

        TopicPartition tp = new TopicPartition(topic, partition);

        return new RecordMetadata(tp, offset, 0, msg.getPublishTime(), 0, msg.hasKey() ? msg.getKey().length() : 0,
                msg.getData().length);

    }
}
