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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Simple Kafka Source to transfer messages from a Kafka topic
 */
public abstract class KafkaAbstractSource<V> extends PushSource<V> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAbstractSource.class);

    private Consumer<String, byte[]> consumer;
    private Properties props;
    private KafkaSourceConfig kafkaSourceConfig;
    Thread runnerThread;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        kafkaSourceConfig = KafkaSourceConfig.load(config);
        if (kafkaSourceConfig.getTopic() == null
                || kafkaSourceConfig.getBootstrapServers() == null
                || kafkaSourceConfig.getGroupId() == null
                || kafkaSourceConfig.getFetchMinBytes() == 0
                || kafkaSourceConfig.getAutoCommitIntervalMs() == 0
                || kafkaSourceConfig.getSessionTimeoutMs() == 0) {
            throw new IllegalArgumentException("Required property not set.");
        }

        props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSourceConfig.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaSourceConfig.getGroupId());
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, kafkaSourceConfig.getFetchMinBytes().toString());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, kafkaSourceConfig.getAutoCommitIntervalMs().toString());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaSourceConfig.getSessionTimeoutMs().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaSourceConfig.getKeyDeserializationClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaSourceConfig.getValueDeserializationClass());

        this.start();

    }

    protected Properties beforeCreateConsumer(Properties props) {
        return props;
    }

    @Override
    public void close() throws InterruptedException {
        LOG.info("Stopping kafka source");
        if (runnerThread != null) {
            runnerThread.interrupt();
            runnerThread.join();
            runnerThread = null;
        }
        if(consumer != null) {
            consumer.close();
            consumer = null;
        }
        LOG.info("Kafka source stopped.");
    }

    public void start() {
        runnerThread = new Thread(() -> {
            LOG.info("Starting kafka source");
            consumer = new KafkaConsumer<>(beforeCreateConsumer(props));
            consumer.subscribe(Arrays.asList(kafkaSourceConfig.getTopic()));
            LOG.info("Kafka source started.");
            ConsumerRecords<String, byte[]> consumerRecords;
            while(true){
                consumerRecords = consumer.poll(1000);
                CompletableFuture<?>[] futures = new CompletableFuture<?>[consumerRecords.count()];
                int index = 0;
                for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
                    LOG.debug("Record received from kafka, key: {}. value: {}", consumerRecord.key(), consumerRecord.value());
                    KafkaRecord<V> record = new KafkaRecord<>(consumerRecord, extractValue(consumerRecord));
                    consume(record);
                    futures[index] = record.getCompletableFuture();
                    index++;
                }
                if (!kafkaSourceConfig.isAutoCommitEnabled()) {
                    try {
                        CompletableFuture.allOf(futures).get();
                        consumer.commitSync();
                    } catch (ExecutionException | InterruptedException ex) {
                        break;
                    }
                }
            }

        });
        runnerThread.setName("Kafka Source Thread");
        runnerThread.start();
    }

    public abstract V extractValue(ConsumerRecord<String, byte[]> record);

    static private class KafkaRecord<V> implements Record<V> {
        private final ConsumerRecord<String, byte[]> record;
        private final V value;
        @Getter
        private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        public KafkaRecord(ConsumerRecord<String, byte[]> record,
                           V value) {
            this.record = record;
            this.value = value;
        }
        @Override
        public Optional<String> getPartitionId() {
            return Optional.of(Integer.toString(record.partition()));
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
    }
}
