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
package org.apache.pulsar.tests.integration.io.sources;

import static org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase.randomName;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/**
 * A tester for testing kafka source.
 */
@Slf4j
public class KafkaSourceTester extends SourceTester<KafkaContainer> {

    private static final String SOURCE_TYPE = "kafka";

    private final String kafkaTopicName;

    private KafkaContainer kafkaContainer;

    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaSourceTester(String containerName) {
        super(SOURCE_TYPE);
        String suffix = randomName(8) + "_" + System.currentTimeMillis();
        this.kafkaTopicName = "kafka_source_topic_" + suffix;

        sourceConfig.put("bootstrapServers", containerName + ":9092");
        sourceConfig.put("groupId", "test-source-group");
        sourceConfig.put("fetchMinBytes", 1L);
        sourceConfig.put("autoCommitIntervalMs", 10L);
        sourceConfig.put("sessionTimeoutMs", 10000L);
        sourceConfig.put("heartbeatIntervalMs", 5000L);
        sourceConfig.put("topic", kafkaTopicName);
        sourceConfig.put("valueDeserializationClass", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }

    @Override
    public void setServiceContainer(KafkaContainer container) {
        this.kafkaContainer = container;
    }

    @Override
    public void prepareSource() throws Exception {
        ExecResult execResult = kafkaContainer.execInContainer(
            "/usr/bin/kafka-topics",
            "--create",
            "--zookeeper",
            "localhost:2181",
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--topic",
            kafkaTopicName);
        assertTrue(
            execResult.getStdout().contains("Created topic"),
            execResult.getStdout());

        kafkaConsumer = new KafkaConsumer<>(
            ImmutableMap.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "source-test-" + randomName(8),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
            ),
            new StringDeserializer(),
            new StringDeserializer()
        );
        kafkaConsumer.subscribe(Arrays.asList(kafkaTopicName));
        log.info("Successfully subscribe to kafka topic {}", kafkaTopicName);
    }

    @Override
    public void prepareInsertEvent() throws Exception {
        // pass
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        // pass
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        // pass
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception{
        KafkaProducer<String, String> producer = new KafkaProducer<>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
        LinkedHashMap<String, String> kvs = new LinkedHashMap<>();
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(
                kafkaTopicName,
                key,
                value
            );
            kvs.put(key, value);
            producer.send(record).get();
        }

        log.info("Successfully produced {} messages to kafka topic {}", numMessages, kafkaTopicName);
        return kvs;
    }
}
