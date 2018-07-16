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
package org.apache.pulsar.tests.containers;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

/**
 * Cassandra Container.
 */
@Slf4j
public class KafkaContainer<SelfT extends ChaosContainer<SelfT>> extends ChaosContainer<SelfT> {

    public static final String NAME = "kafka";
    public static final int INTERNAL_PORT = 9092;
    public static final int PORT = 9093;

    public KafkaContainer(String clusterName) {
        super(clusterName, "confluentinc/cp-kafka:4.1.1");
    }

    @Override
    protected void configure() {
        super.configure();
        this.withNetworkAliases(NAME)
            .withExposedPorts(INTERNAL_PORT, PORT)
            .withClasspathResourceMapping(
                "kafka-zookeeper.properties", "/zookeeper.properties",
                BindMode.READ_ONLY)
            .withCommand("sh", "-c", "zookeeper-server-start /zookeeper.properties & /etc/confluent/docker/run")
            .withEnv("KAFKA_LISTENERS",
                "INTERNAL://kafka:" + INTERNAL_PORT + ",PLAINTEXT://" + "0.0.0.0" + ":" + PORT)
            .withEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:2181")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
            .withEnv("KAFKA_BROKER_ID", "1")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
            .withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "")
            .withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(NAME);
                createContainerCmd.withName(clusterName + "-" + NAME);
            })
            .waitingFor(new HostPortWaitStrategy());
    }
}
