/*
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
package org.apache.pulsar.tests.integration.messaging;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.integration.GeoRepIntegTest;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.Assert;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Geo replication test.
 */
@Slf4j
public class GeoReplication extends GeoRepIntegTest {

    public GeoReplication(PulsarClient clientA, PulsarAdmin adminA, PulsarClient clientB, PulsarAdmin adminB) {
        super(clientA, adminA, clientB, adminB);
    }

    public void testTopicReplication(String domain) throws Exception {

        String topic = domain + "://public/default/testTopicReplication-" + UUID.randomUUID();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            try {
                adminA.topics().createPartitionedTopic(topic, 10);
            } catch (Exception e) {
                log.error("Failed to create partitioned topic {}.", topic, e);
                Assert.fail("Failed to create partitioned topic " + topic);
            }
            Assert.assertEquals(adminA.topics().getPartitionedTopicMetadata(topic).partitions, 10);
        });
        log.info("Test geo-replication produce and consume for topic {}.", topic);

        @Cleanup
        Producer<byte[]> p = clientA.newProducer()
                .topic(topic)
                .create();
        log.info("Successfully create producer in cluster {} for topic {}.", "cluster1", topic);

        @Cleanup
        Consumer<byte[]> c = clientB.newConsumer()
                .topic(topic)
                .subscriptionName("geo-sub")
                .subscribe();
        log.info("Successfully create consumer in cluster {} for topic {}.", "cluster2", topic);

        for (int i = 0; i < 10; i++) {
            p.send(String.format("Message [%d]", i).getBytes(StandardCharsets.UTF_8));
        }
        log.info("Successfully produce message to cluster {} for topic {}.", "cluster1", topic);

        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = c.receive(10, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
        }
        log.info("Successfully consume message from cluster {} for topic {}.", "cluster2", topic);
    }
}
