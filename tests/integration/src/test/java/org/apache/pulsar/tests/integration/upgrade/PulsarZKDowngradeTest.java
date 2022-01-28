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
package org.apache.pulsar.tests.integration.upgrade;

import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.ImmutableMap;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test downgrading ZK from 3.5.x to 3.4.x. This is part of the upgrade from Pulsar 2.1.0 to 2.1.1.
 */
@Slf4j
public class PulsarZKDowngradeTest extends PulsarClusterTestBase {

    protected static final int ENTRIES_PER_LEDGER = 1024;

    @BeforeClass(alwaysRun = true)
    @Override
    public final void setupCluster() throws Exception {
        incrementSetupNumber();

        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .numBookies(2)
                .numBrokers(1)
                .clusterName(clusterName)
                .classPathVolumeMounts(
                        ImmutableMap.<String, String> builder()
                                .put("zk-3.5-test-data", "/pulsar/data/zookeeper/version-2/version-2")
                                .build())
                .build();

        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    @AfterClass(alwaysRun = true)
    @Override
    public final void tearDownCluster() throws Exception {
        super.tearDownCluster();
    }

    @Test(dataProvider = "ServiceUrlAndTopics")
    public void testPublishAndConsume(Supplier<String> serviceUrl, boolean isPersistent) throws Exception {
        String topicName = generateTopicName("testpubconsume", isPersistent);

        int numMessages = 10;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl.get())
                .build();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscribe();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .create();

        for (int i = 0; i < numMessages; i++) {
            producer.send("smoke-message-" + i);
        }

        for (int i = 0; i < numMessages; i++) {
            Message<String> m = consumer.receive();
            assertEquals("smoke-message-" + i, m.getValue());
        }

    }
}
