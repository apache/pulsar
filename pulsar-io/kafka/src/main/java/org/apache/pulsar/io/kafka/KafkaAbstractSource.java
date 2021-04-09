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

package org.apache.pulsar.io.kafka;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Simple Kafka Source to transfer messages from a Kafka topic.
 */
public abstract class KafkaAbstractSource<V> extends PushSource<V> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAbstractSource.class);

    private volatile Consumer<Object, Object> consumer;
    private volatile boolean running = false;
    private KafkaSourceConfig kafkaSourceConfig;
    private Thread runnerThread;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        kafkaSourceConfig = KafkaSourceConfig.load(config);
        Objects.requireNonNull(kafkaSourceConfig.getTopic(), "Kafka topic is not set");
        Objects.requireNonNull(kafkaSourceConfig.getBootstrapServers(), "Kafka bootstrapServers is not set");
        Objects.requireNonNull(kafkaSourceConfig.getGroupId(), "Kafka consumer group id is not set");
        if (kafkaSourceConfig.getFetchMinBytes() <= 0) {
            throw new IllegalArgumentException("Invalid Kafka Consumer fetchMinBytes : "
                + kafkaSourceConfig.getFetchMinBytes());
        }
        if (kafkaSourceConfig.isAutoCommitEnabled() && kafkaSourceConfig.getAutoCommitIntervalMs() <= 0) {
            throw new IllegalArgumentException("Invalid Kafka Consumer autoCommitIntervalMs : "
                + kafkaSourceConfig.getAutoCommitIntervalMs());
        }
        if (kafkaSourceConfig.getSessionTimeoutMs() <= 0) {
            throw new IllegalArgumentException("Invalid Kafka Consumer sessionTimeoutMs : "
                + kafkaSourceConfig.getSessionTimeoutMs());
        }
        if (kafkaSourceConfig.getHeartbeatIntervalMs() <= 0) {
            throw new IllegalArgumentException("Invalid Kafka Consumer heartbeatIntervalMs : "
                    + kafkaSourceConfig.getHeartbeatIntervalMs());
        }

        Properties props = new Properties();
        if (kafkaSourceConfig.getConsumerConfigProperties() != null) {
            props.putAll(kafkaSourceConfig.getConsumerConfigProperties());
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSourceConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaSourceConfig.getGroupId());
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, String.valueOf(kafkaSourceConfig.getFetchMinBytes()));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(kafkaSourceConfig.getAutoCommitIntervalMs()));
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(kafkaSourceConfig.getSessionTimeoutMs()));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(kafkaSourceConfig.getHeartbeatIntervalMs()));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaSourceConfig.getAutoOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaSourceConfig.getKeyDeserializationClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaSourceConfig.getValueDeserializationClass());
        try {
            consumer = new KafkaConsumer<>(beforeCreateConsumer(props));
        } catch (Exception ex) {
            throw new IllegalArgumentException("Unable to instantiate Kafka consumer", ex);
        }
        this.start();
        running = true;
    }

    protected Properties beforeCreateConsumer(Properties props) {
        return props;
    }

    @Override
    public void close() throws InterruptedException {
        LOG.info("Stopping kafka source");
        running = false;
        if (runnerThread != null) {
            runnerThread.interrupt();
            runnerThread.join();
            runnerThread = null;
        }
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        LOG.info("Kafka source stopped.");
    }

    @SuppressWarnings("unchecked")
    public void start() {
        runnerThread = new Thread(() -> {
            LOG.info("Starting kafka source on {}", kafkaSourceConfig.getTopic());
            consumer.subscribe(Collections.singletonList(kafkaSourceConfig.getTopic()));
            LOG.info("Kafka source started.");
            while (running) {
                ConsumerRecords<Object, Object> consumerRecords = consumer.poll(1000);
                CompletableFuture<?>[] futures = new CompletableFuture<?>[consumerRecords.count()];
                int index = 0;
                for (ConsumerRecord<Object, Object> consumerRecord : consumerRecords) {
                    KafkaRecord record = buildRecord(consumerRecord);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Write record {} {} {}", record.getKey(), record.getValue(), record.getSchema());
                    }
                    consume(record);
                    futures[index] = record.getCompletableFuture();
                    index++;
                }
                if (!kafkaSourceConfig.isAutoCommitEnabled()) {
                    try {
                        CompletableFuture.allOf(futures).get();
                        consumer.commitSync();
                    } catch (InterruptedException ex) {
                        break;
                    } catch (ExecutionException ex) {
                        LOG.error("Error while processing records", ex);
                        break;
                    }
                }
            }
        });
        runnerThread.setUncaughtExceptionHandler((t, e) -> LOG.error("[{}] Error while consuming records", t.getName(), e));
        runnerThread.setName("Kafka Source Thread");
        runnerThread.start();
    }

    public abstract KafkaRecord buildRecord(ConsumerRecord<Object, Object> consumerRecord);

    @Slf4j
    protected static class KafkaRecord<V> implements Record<V> {
        private final ConsumerRecord<String, ?> record;
        private final V value;
        private final Schema<V> schema;

        @Getter
        private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        public KafkaRecord(ConsumerRecord<String,?> record, V value, Schema<V> schema) {
            this.record = record;
            this.value = value;
            this.schema = schema;
        }
        @Override
        public Optional<String> getPartitionId() {
            return Optional.of(Integer.toString(record.partition()));
        }

        @Override
        public Optional<Integer> getPartitionIndex() {
            return Optional.of(record.partition());
        }

        @Override
        public Optional<Long> getRecordSequence() {
            return Optional.of(record.offset());
        }

        @Override
        public Optional<String> getKey() {
            return Optional.ofNullable(record.key());
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public void ack() {
            completableFuture.complete(null);
        }

        @Override
        public Schema<V> getSchema() {
            return schema;
        }
    }
    protected static class KeyValueKafkaRecord<V> extends KafkaRecord implements KVRecord<Object, Object> {

        private final Schema<Object> keySchema;
        private final Schema<Object> valueSchema;

        public KeyValueKafkaRecord(ConsumerRecord record, KeyValue value,
                                   Schema<Object> keySchema, Schema<Object> valueSchema) {
            super(record, value, null);
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
        }

        @Override
        public Schema<Object> getKeySchema() {
            return keySchema;
        }

        @Override
        public Schema<Object> getValueSchema() {
            return valueSchema;
        }

        @Override
        public KeyValueEncodingType getKeyValueEncodingType() {
            return KeyValueEncodingType.SEPARATED;
        }
    }
}
