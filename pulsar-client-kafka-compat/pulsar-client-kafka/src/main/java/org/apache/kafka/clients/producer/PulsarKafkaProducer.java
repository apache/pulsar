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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.kafka.compat.MessageIdUtils;
import org.apache.pulsar.client.kafka.compat.PulsarClientKafkaConfig;
import org.apache.pulsar.client.kafka.compat.PulsarProducerKafkaConfig;

public class PulsarKafkaProducer<K, V> implements Producer<K, V> {

    private final PulsarClient client;
    private final ProducerBuilder<byte[]> pulsarProducerBuilder;

    private final ConcurrentMap<String, org.apache.pulsar.client.api.Producer<byte[]>> producers = new ConcurrentHashMap<>();

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    /** Map that contains the last future for each producer */
    private final ConcurrentMap<String, CompletableFuture<MessageId>> lastSendFuture = new ConcurrentHashMap<>();

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
        try {
            client = PulsarClientKafkaConfig.getClientBuilder(properties).serviceUrl(serviceUrl).build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        pulsarProducerBuilder = PulsarProducerKafkaConfig.getProducerBuilder(client, properties);

        // To mimic the same batching mode as Kafka, we need to wait a very little amount of
        // time to batch if the client is trying to send messages fast enough
        long lingerMs = Long.parseLong(properties.getProperty(ProducerConfig.LINGER_MS_CONFIG, "1"));
        pulsarProducerBuilder.batchingMaxPublishDelay(lingerMs, TimeUnit.MILLISECONDS);

        String compressionType = properties.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG);
        if ("gzip".equals(compressionType)) {
            pulsarProducerBuilder.compressionType(CompressionType.ZLIB);
        } else if ("lz4".equals(compressionType)) {
            pulsarProducerBuilder.compressionType(CompressionType.LZ4);
        }


        int sendTimeoutMillis = Integer.parseInt(properties.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000"));
        pulsarProducerBuilder.sendTimeout(sendTimeoutMillis, TimeUnit.MILLISECONDS);

        boolean blockOnBufferFull = Boolean
                .parseBoolean(properties.getProperty(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false"));

        // Kafka blocking semantic when blockOnBufferFull=false is different from Pulsar client
        // Pulsar throws error immediately when the queue is full and blockIfQueueFull=false
        // Kafka, on the other hand, still blocks for "max.block.ms" time and then gives error.
        boolean shouldBlockPulsarProducer = sendTimeoutMillis > 0 || blockOnBufferFull;
        pulsarProducerBuilder.blockIfQueueFull(shouldBlockPulsarProducer);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        org.apache.pulsar.client.api.Producer<byte[]> producer;

        try {
            producer = producers.computeIfAbsent(record.topic(), topic -> createNewProducer(topic));
        } catch (Exception e) {
            if (callback != null) {
                callback.onCompletion(null, e);
            }
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }

        TypedMessageBuilder<byte[]> messageBuilder = producer.newMessage();
        int messageSize = buildMessage(messageBuilder, record);;

        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        CompletableFuture<MessageId> sendFuture = messageBuilder.sendAsync();
        lastSendFuture.put(record.topic(), sendFuture);

        sendFuture.thenAccept((messageId) -> {
            future.complete(getRecordMetadata(record.topic(), messageBuilder, messageId, messageSize));
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });

        future.handle((recordMetadata, throwable) -> {
            if (callback != null) {
                Exception exception = throwable != null ? new Exception(throwable) : null;
                callback.onCompletion(recordMetadata, exception);
            }
            return null;
        });

        return future;
    }

    @Override
    public void flush() {
        lastSendFuture.forEach((topic, future) -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            // Remove the futures to remove eventually failed operations in order to trigger errors only once
            lastSendFuture.remove(topic, future);
        });
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.emptyMap();
    }

    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        try {
            client.closeAsync().get(timeout, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private org.apache.pulsar.client.api.Producer<byte[]> createNewProducer(String topic) {
        try {
            return pulsarProducerBuilder.clone().topic(topic).create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private int buildMessage(TypedMessageBuilder<byte[]> builder, ProducerRecord<K, V> record) {
        if (record.partition() != null) {
            throw new UnsupportedOperationException("");
        }

        if (record.key() != null) {
            builder.key(getKey(record.topic(), record.key()));
        }

        if (record.timestamp() != null) {
            builder.eventTime(record.timestamp());
        }

        byte[] value = valueSerializer.serialize(record.topic(), record.value());
        builder.value(value);
        return value.length;
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

    private RecordMetadata getRecordMetadata(String topic, TypedMessageBuilder<byte[]> msgBuilder, MessageId messageId,
            int size) {
        MessageIdImpl msgId = (MessageIdImpl) messageId;

        // Combine ledger id and entry id to form offset
        long offset = MessageIdUtils.getOffset(msgId);
        int partition = msgId.getPartitionIndex();

        TopicPartition tp = new TopicPartition(topic, partition);
        TypedMessageBuilderImpl<byte[]> mb = (TypedMessageBuilderImpl<byte[]>) msgBuilder;
        return new RecordMetadata(tp, offset, 0, mb.getPublishTime(), 0, mb.hasKey() ? mb.getKey().length() : 0, size);
    }
}
