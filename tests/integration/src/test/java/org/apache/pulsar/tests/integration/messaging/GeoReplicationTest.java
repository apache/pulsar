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
package org.apache.pulsar.tests.integration.messaging;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tests.integration.topologies.PulsarGeoClusterTestBase;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class GeoReplicationTest extends PulsarGeoClusterTestBase {

    @BeforeClass(alwaysRun = true)
    public final void setupBeforeClass() throws Exception {
        setup();
    }

    @AfterClass(alwaysRun = true)
    public final void tearDownAfterClass() throws Exception {
        cleanup();
    }

    @Test(timeOut = 600000, dataProvider = "TopicDomain")
    public void testTopicReplication(String domain) throws Exception {

        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(getGeoCluster().getClusters()[0].getHttpServiceUrl())
                .requestTimeout(30, TimeUnit.SECONDS)
                .build();

        String[] topics = new String[1];
        Awaitility.await().atMost(150, TimeUnit.SECONDS).untilAsserted(() -> {
            String topic = domain + "://public/default/testNonPersistentTopicReplication-" + UUID.randomUUID();
            try {
                admin.topics().createPartitionedTopic(topic, 10);
            } catch (Exception ignore) {
            }
            Assert.assertEquals(admin.topics().getPartitionedTopicMetadata(topic).partitions, 10);
            topics[0] = topic;
        });

        final String topic = topics[0];

        PulsarClient client1 = PulsarClient.builder()
                .serviceUrl(getGeoCluster().getClusters()[0].getPlainTextServiceUrl())
                .build();

        PulsarClient client2 = PulsarClient.builder()
                .serviceUrl(getGeoCluster().getClusters()[1].getPlainTextServiceUrl())
                .build();

        Producer<byte[]> p = client1.newProducer()
                .topic(topic)
                .create();

        Consumer<byte[]> c = client2.newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();

        for (int i = 0; i < 10; i++) {
            p.send(String.format("Message [%d]", i).getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < 10; i++) {
            c.receive();
        }

        c.close();
        p.close();
        client2.close();
        client1.close();
        admin.close();
    }
}
