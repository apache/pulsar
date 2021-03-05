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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkTask;
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
        KeyValue<Schema, Schema> keyValueSchemas = extractKeyValueSchemas(sourceRecord);

        ProducerRecord<K, V> record = new ProducerRecordWithSchema<K, V>(kafkaSinkConfig.getTopic(),
                keyValue.getKey(), keyValue.getValue(),
                keyValueSchemas.getKey(), keyValueSchemas.getValue());
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
        // kafkaSinkConfig.setKafkaConnectorSinkClass("org.apache.kafka.connect.file.FileStreamSinkConnector");
        String kafkaConnectorName = kafkaSinkConfig.getKafkaConnectorSinkClass();
        if (Strings.isNullOrEmpty(kafkaConnectorName)) {
            Objects.requireNonNull(kafkaSinkConfig.getTopic(), "Kafka topic is not set");
            Objects.requireNonNull(kafkaSinkConfig.getBootstrapServers(), "Kafka bootstrapServers is not set");
            Objects.requireNonNull(kafkaSinkConfig.getAcks(), "Kafka acks mode is not set");
            if (kafkaSinkConfig.getBatchSize() <= 0) {
                throw new IllegalArgumentException("Invalid Kafka Producer batchSize : "
                        + kafkaSinkConfig.getBatchSize());
            }
            if (kafkaSinkConfig.getMaxRequestSize() <= 0) {
                throw new IllegalArgumentException("Invalid Kafka Producer maxRequestSize : "
                        + kafkaSinkConfig.getMaxRequestSize());
            }
            if (kafkaSinkConfig.getProducerConfigProperties() != null) {
                props.putAll(kafkaSinkConfig.getProducerConfigProperties());
            }

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkConfig.getBootstrapServers());
            props.put(ProducerConfig.ACKS_CONFIG, kafkaSinkConfig.getAcks());
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(kafkaSinkConfig.getBatchSize()));
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(kafkaSinkConfig.getMaxRequestSize()));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaSinkConfig.getKeySerializerClass());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaSinkConfig.getValueSerializerClass());

            producer = new KafkaProducer<>(beforeCreateProducer(props));
        } else {
            String prefix = kafkaSinkConfig.getKafkaConnectorConfigPrefix();
            config.entrySet().stream()
                    .filter(kv -> kv.getKey().startsWith(prefix))
                    .forEach(kv -> {
                        String name = kv.getKey().substring(prefix.length());
                        props.put(name, kv.getValue());
                    });

            // props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaSinkConfig.getKeySerializerClass());
            // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaSinkConfig.getValueSerializerClass());

            // todo: schemas from config

            producer = createKafkaSinkWrappingProducer(kafkaConnectorName,
                            props,
                            Schema.STRING_SCHEMA,
                            Schema.BYTES_SCHEMA);
        }
        log.info("Kafka sink started : {}.", props);
    }

    public static <K, V> Producer<K,V> createKafkaSinkWrappingProducer(String kafkaConnectorName,
                                                          Properties props,
                                                          Schema keySchema,
                                                          Schema valueSchema) {
        try {
            Class<?> clazz = Class.forName(kafkaConnectorName);
            SinkConnector connector = (SinkConnector)clazz.getConstructor().newInstance();

            Class<? extends Task> taskClass = connector.taskClass();
            SinkContextSim sinkContext = new SinkContextSim();
            connector.initialize(sinkContext);
            connector.start(Maps.fromProperties(props));

            List<Map<String, String>> configs = connector.taskConfigs(1);
            SinkTask task = (SinkTask)taskClass.getConstructor().newInstance();
            SinkTaskContextSim taskContext =
                    new SinkTaskContextSim(configs.get(0), KafkaSinkWrappingProducer.getPartitions());
            task.initialize(taskContext);
            task.start(configs.get(0));
            task.open(KafkaSinkWrappingProducer.getPartitions());

            Producer<K, V> producer = new KafkaSinkWrappingProducer<>(connector, task,
                    keySchema, valueSchema,
                    sinkContext, taskContext);
            return producer;
        } catch (Exception e) {
            log.error("Failed to create KafkaSinkWrappingProducer with {}, {} & {}",
                    props, keySchema.name(), valueSchema.name(), e);
            throw new IllegalArgumentException("failed to create KafkaSink with given parameters", e);
        }
    }

    public abstract KeyValue<K, V> extractKeyValue(Record<byte[]> message);

    public abstract KeyValue<Schema, Schema> extractKeyValueSchemas(Record<byte[]> message);
}