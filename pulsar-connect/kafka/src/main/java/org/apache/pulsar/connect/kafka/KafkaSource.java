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

package org.apache.pulsar.connect.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.pulsar.connect.core.Message;
import org.apache.pulsar.connect.core.PushSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Simple Kafka Source to transfer messages from a Kafka topic
 */
public class KafkaSource<V> implements PushSource<V> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

    private Consumer<String, V> consumer;
    private Properties props;
    private KafkaSourceConfig kafkaSourceConfig;
    Thread runnerThread;

    private java.util.function.Function<Message<V>, CompletableFuture<Void>> consumeFunction;

    @Override
    public void open(Map<String, String> config) throws Exception {
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

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaSourceConfig.getKeyDeserializationClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaSourceConfig.getValueDeserializationClass());

        this.start();

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
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(kafkaSourceConfig.getTopic()));
            LOG.info("Kafka source started.");
            ConsumerRecords<String, V> records;
            while(true){
                records = consumer.poll(1000);
                CompletableFuture<?>[] futures = new CompletableFuture<?>[records.count()];
                int index = 0;
                for (ConsumerRecord<String, V> record : records) {
                    LOG.debug("Message received from kafka, key: {}. value: {}", record.key(), record.value());
                    futures[index] = consumeFunction.apply(new KafkaMesssage<>(record));
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

    @Override
    public void setConsumer(java.util.function.Function<Message<V>, CompletableFuture<Void>> consumeFunction) {
        this.consumeFunction = consumeFunction;
    }

    static private class KafkaMesssage<V> implements Message<V>  {
        ConsumerRecord<String, V> record;

        public KafkaMesssage(ConsumerRecord<String, V> record) {
            this.record = record;

        }
        @Override
        public String getPartitionId() {
            return Integer.toString(record.partition());
        }

        @Override
        public Long getSequenceId() {
            return record.offset();
        }

        @Override
        public V getData() {
            return record.value();
        }
    }
}