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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * A Simple abstract class for Kafka sink
 * Users need to implement extractKeyValue function to use this sink
 */
@Slf4j
public abstract class KafkaAbstractSink<K, V> implements Sink<byte[]> {

    private Producer<K, V> producer;
    private Properties props = new Properties();
    private KafkaSinkConfig kafkaSinkConfig;

    @Override
    public void write(Record<byte[]> sourceRecord) {
        KeyValue<K, V> keyValue = extractKeyValue(sourceRecord);
        ProducerRecord<K, V> record = new ProducerRecord<>(kafkaSinkConfig.getTopic(), keyValue.getKey(), keyValue.getValue());
        if (log.isDebugEnabled()) {
            log.debug("Record sending to kafka, record={}.", record);
        }

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                sourceRecord.ack();
            } else {
                sourceRecord.fail();
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
            log.info("Kafka sink stopped.");
        }
    }

    protected Properties beforeCreateProducer(Properties props) {
        return props;
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
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

        producer = new KafkaProducer<>(beforeCreateProducer(props));

        log.info("Kafka sink started.");
    }

    public abstract KeyValue<K, V> extractKeyValue(Record<byte[]> message);
}