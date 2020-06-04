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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import com.google.common.annotations.VisibleForTesting;

//Questions
/**
 * 1. What's the auth method
 * 2. What if message publish fails with async
 */

import kafka.javaapi.producer.Producer;
import kafka.producer.DefaultPartitioner;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;
import kafka.serializer.StringEncoder;
import kafka.utils.VerifiableProperties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarKafkaProducer<K, V> extends Producer<K, V> {

    private final PulsarClient client;
    private final ProducerBuilder<byte[]> pulsarProducerBuilder;
    private final Partitioner partitioner;
    private final Encoder<K> keySerializer;
    private final Encoder<V> valueSerializer;
    private final boolean isSendAsync;

    public static String KAFKA_KEY_MAX_QUEUE_BUFFERING_TIME_MS = "queue.buffering.max.ms";
    public static String KAFKA_KEY_MAX_QUEUE_BUFFERING_MESSAGES = "queue.buffering.max.messages";
    public static String KAFKA_KEY_MAX_BATCH_MESSAGES = "batch.num.messages";
    public static String KAFKA_KEY_REQUEST_TIMEOUT_MS = "request.timeout.ms";

    private final ConcurrentMap<String, org.apache.pulsar.client.api.Producer<byte[]>> producers = new ConcurrentHashMap<>();

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public PulsarKafkaProducer(ProducerConfig config) {
        super((kafka.producer.Producer) null);
        partitioner = config.partitionerClass() != null
                ? newInstance(config.partitionerClass(), Partitioner.class, config.props())
                : new DefaultPartitioner(config.props());
        // kafka-config returns default serializer if client doesn't configure it
        checkNotNull(config.keySerializerClass(), "key-serializer class can't be null");
        checkNotNull(config.serializerClass(), "value-serializer class can't be null");
        keySerializer = newInstance(config.keySerializerClass(), Encoder.class, config.props());
        valueSerializer = newInstance(config.serializerClass(), Encoder.class, config.props());

        Properties properties = config.props() != null && config.props().props() != null ? config.props().props()
                : new Properties();
        String serviceUrl = config.brokerList();
        try {
            client = PulsarClientKafkaConfig.getClientBuilder(properties).serviceUrl(serviceUrl).build();
        } catch (PulsarClientException e) {
            throw new IllegalArgumentException(
                    "Failed to create pulsar-client using url = " + serviceUrl + ", properties = " + properties, e);
        }
        pulsarProducerBuilder = client.newProducer();

        // doc: https://kafka.apache.org/08/documentation.html#producerapi
        // api-doc:
        // https://github.com/apache/kafka/blob/0.8.2.2/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java

        // queue.enqueue.timeout.ms: The amount of time to block before dropping messages when running in async mode and
        // the buffer has reached queue.buffering.max.messages. If set to 0 events will be enqueued immediately or
        // dropped if the queue is full (the producer send call will never block). If set to -1 the producer will block
        // indefinitely and never willingly drop a send.
        boolean blockIfQueueFull = config.queueEnqueueTimeoutMs() == -1 ? true : false;
        // This parameter specifies whether the messages are sent asynchronously in a background thread. Valid values
        // are (1) async for asynchronous send and (2) sync for synchronous send. By setting the producer to async we
        // allow batching together of requests (which is great for throughput) but open the possibility of a failure of
        // the client machine dropping unsent data.
        isSendAsync = "async".equalsIgnoreCase(config.producerType());
        CompressionType compressionType = CompressionType.NONE;
        // Valid values are "none", "gzip" and "snappy".
        if ("gzip".equals(config.compressionCodec().name())) {
            compressionType = CompressionType.ZLIB;
        } else if ("snappy".equals(config.compressionCodec().name())) {
            compressionType = CompressionType.SNAPPY;
        }
        long batchDelayMs = config.queueBufferingMaxMs();

        if (properties.containsKey(KAFKA_KEY_MAX_QUEUE_BUFFERING_MESSAGES)) {
            pulsarProducerBuilder.maxPendingMessages(config.queueBufferingMaxMessages());
        }
        if (properties.containsKey(KAFKA_KEY_MAX_BATCH_MESSAGES)) {
            pulsarProducerBuilder.batchingMaxMessages(config.batchNumMessages());
        }
        if (properties.containsKey(KAFKA_KEY_MAX_QUEUE_BUFFERING_TIME_MS)) {
            pulsarProducerBuilder.batchingMaxPublishDelay(batchDelayMs, TimeUnit.MILLISECONDS);
        }
        if (properties.containsKey(KAFKA_KEY_REQUEST_TIMEOUT_MS)) {
            pulsarProducerBuilder.sendTimeout(config.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        }

        pulsarProducerBuilder.blockIfQueueFull(blockIfQueueFull).compressionType(compressionType);

    }

    public PulsarKafkaProducer(kafka.producer.Producer<K, V> producer) {
        this(producer.config());
    }

    @Override
    public void send(KeyedMessage<K, V> message) {
        org.apache.pulsar.client.api.Producer<byte[]> producer;

        try {
            producer = producers.computeIfAbsent(message.topic(), topic -> createNewProducer(topic));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create producer for " + message.topic(), e);
        }

        TypedMessageBuilder<byte[]> messageBuilder = producer.newMessage();
        buildMessage(messageBuilder, message);

        if (isSendAsync) {
            // what if message publish fails:
            // according to : https://kafka.apache.org/08/documentation.html#producerapi
            // async: opens the possibility of a failure of the client machine dropping unsent data
            messageBuilder.sendAsync().handle((res, ex) -> {
                if (ex != null) {
                    log.warn("publish failed for {}", producer.getTopic(), ex);
                }
                return null;
            });
        } else {
            try {
                messageBuilder.send();
            } catch (PulsarClientException e) {
                log.warn("publish failed for {}", producer.getTopic(), e);
                throw new IllegalStateException("Failed to publish message " + message.topic(), e);
            }
        }

    }

    @Override
    public void send(List<KeyedMessage<K, V>> messages) {
        if (messages != null) {
            messages.forEach(this::send);
        }
    }

    private void buildMessage(TypedMessageBuilder<byte[]> builder, KeyedMessage<K, V> message) {
        if (message.key() != null) {
            String key = getKey(message.topic(), message.key());
            builder.key(key);
        }

        byte[] value = valueSerializer.toBytes(message.message());
        builder.value(value);
    }

    private String getKey(String topic, K key) {
        // If key is a String, we can use it as it is, otherwise, serialize to byte[] and encode in base64
        if (keySerializer!=null && keySerializer instanceof StringEncoder) {
            return (String) key;
        } else {
            byte[] keyBytes = keySerializer.toBytes(key);
            return Base64.getEncoder().encodeToString(keyBytes);
        }
    }

    private org.apache.pulsar.client.api.Producer<byte[]> createNewProducer(String topic) {
        try {
            pulsarProducerBuilder.messageRoutingMode(MessageRoutingMode.CustomPartition);
            pulsarProducerBuilder.messageRouter(new MessageRouter() {
                private static final long serialVersionUID = 1L;

                @Override
                public int choosePartition(Message<?> msg, TopicMetadata metadata) {
                    // https://kafka.apache.org/08/documentation.html#producerapi
                    // The default partitioner is based on the hash of the key.
                    return partitioner.partition(msg.getKey(), metadata.numPartitions());
                }
            });
            log.info("Creating producer for topic {} with config {}", topic, pulsarProducerBuilder.toString());
            return pulsarProducerBuilder.clone().topic(topic).create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T newInstance(String key, Class<T> t, VerifiableProperties properties) {
        Class<?> c = null;
        try {
            c = Class.forName(key);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw new IllegalArgumentException("class not found for :" + key);
        }
        if (c == null)
            return null;
        Object o = newInstance(c, properties);
        if (!t.isInstance(o)) {
            throw new IllegalArgumentException(c.getName() + " is not an instance of " + t.getName());
        }
        return t.cast(o);
    }

    public static <T> T newInstance(Class<T> c, VerifiableProperties properties) {
        try {
            try {
                Constructor<T> constructor = c.getConstructor(VerifiableProperties.class);
                constructor.setAccessible(true);
                return constructor.newInstance(properties);
            } catch (Exception e) {
                // Ok.. not a default implementation class
            }
            return c.newInstance();
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Could not instantiate class " + c.getName(), e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(
                    "Could not instantiate class " + c.getName() + " Does it have a public no-argument constructor?",
                    e);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Requested class was null", e);
        }
    }

    @Override
    public void close() {
        if (producers != null) {
            producers.forEach((topic, producer) -> {
                try {
                    producer.close();
                } catch (PulsarClientException e) {
                    log.warn("Failed to close producer for {}: {}", topic, e.getMessage());
                }
            });
        }
        if (client != null) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close pulsar-client", e);
            }
        }
    }

    @VisibleForTesting
    public ProducerBuilder<byte[]> getPulsarProducerBuilder() {
        return pulsarProducerBuilder;
    }

    @VisibleForTesting
    public Partitioner getPartitioner() {
        return partitioner;
    }

    @VisibleForTesting
    public Encoder<K> getKeySerializer() {
        return keySerializer;
    }

    @VisibleForTesting
    public Encoder<V> getValueSerializer() {
        return valueSerializer;
    }
}
