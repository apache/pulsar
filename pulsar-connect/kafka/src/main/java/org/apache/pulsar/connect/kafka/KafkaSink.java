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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.common.util.KeyValue;
import org.apache.pulsar.connect.core.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Simple Kafka Sink to publish messages to a Kafka topic
 */
public class KafkaSink<K, V> implements Sink<KeyValue<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);

    private Producer<K, V> producer;
    private Properties props = new Properties();
    private KafkaSinkConfig kafkaSinkConfig;

    @Override
    public CompletableFuture<Void> write(KeyValue<K, V> message) {
        ProducerRecord<K, V> record = new ProducerRecord<>(kafkaSinkConfig.getTopic(), message.getKey(), message.getValue());
        LOG.debug("Message sending to kafka, record={}.", record);
        Future f = producer.send(record);
        return CompletableFuture.supplyAsync(() -> {
            try {
                f.get();
                return null;
            } catch (InterruptedException|ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        producer.close();
        LOG.info("Kafka sink stopped.");
    }

    @Override
    public void open(Map<String, String> config) throws Exception {
        kafkaSinkConfig = KafkaSinkConfig.load(config);
        if (kafkaSinkConfig.getTopic() == null
                || kafkaSinkConfig.getBootstrapServers() == null
                || kafkaSinkConfig.getAcks() == null
                || kafkaSinkConfig.getBatchSize() == 0
                || kafkaSinkConfig.getMaxRequestSize() == 0) {
            throw new IllegalArgumentException("Required property not set.");
        }

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkConfig.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, kafkaSinkConfig.getAcks());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaSinkConfig.getBatchSize().toString());
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaSinkConfig.getMaxRequestSize().toString());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaSinkConfig.getKeySerializerClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaSinkConfig.getValueSerializerClass());

        producer = new KafkaProducer<>(props);

        LOG.info("Kafka sink started.");
    }
}