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
package org.apache.pulsar.tests.integration.io.sinks;

import static org.apache.pulsar.tests.integration.topologies.PulsarTestBase.randomName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.tests.integration.io.sinks.SinkTester.SinkType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/**
 * A tester for testing kafka sink.
 */
@Slf4j
public class KafkaSinkTester extends SinkTester<KafkaContainer> {

    private final String kafkaTopicName;
    private KafkaConsumer<String, String> kafkaConsumer;

    private final String containerName;

      public KafkaSinkTester(String containerName) {
        super(containerName, SinkType.KAFKA);
        this.containerName = containerName;
        String suffix = randomName(8) + "_" + System.currentTimeMillis();
        this.kafkaTopicName = "kafka_sink_topic_" + suffix;

        sinkConfig.put("bootstrapServers", networkAlias + ":9092");
        sinkConfig.put("acks", "all");
        sinkConfig.put("batchSize", 1L);
        sinkConfig.put("maxRequestSize", 1048576L);
        sinkConfig.put("topic", kafkaTopicName);
    }

    @Override
    protected KafkaContainer createSinkService(PulsarCluster cluster) {
        return new KafkaContainer()
                .withEmbeddedZookeeper()
                .withNetworkAliases(containerName)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                    .withName(containerName)
                    .withHostName(cluster.getClusterName() + "-" + containerName));
    }

    @Override
    public void prepareSink() throws Exception {
        ExecResult execResult = serviceContainer.execInContainer(
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
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serviceContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "sink-test-" + randomName(8),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
            ),
            new StringDeserializer(),
            new StringDeserializer()
        );
        kafkaConsumer.subscribe(Arrays.asList(kafkaTopicName));
        log.info("Successfully subscribe to kafka topic {}", kafkaTopicName);
    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        Iterator<Map.Entry<String, String>> kvIter = kvs.entrySet().iterator();
        while (kvIter.hasNext()) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            log.info("Received {} records from kafka topic {}",
                records.count(), kafkaTopicName);
            if (records.isEmpty()) {
                continue;
            }

            Iterator<ConsumerRecord<String, String>> recordsIter = records.iterator();
            while (recordsIter.hasNext() && kvIter.hasNext()) {
                ConsumerRecord<String, String> consumerRecord = recordsIter.next();
                Map.Entry<String, String> expectedRecord = kvIter.next();
                assertEquals(expectedRecord.getKey(), consumerRecord.key());
                assertEquals(expectedRecord.getValue(), consumerRecord.value());
            }
        }
    }
}
