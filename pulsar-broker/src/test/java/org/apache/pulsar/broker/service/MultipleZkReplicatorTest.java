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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import java.lang.reflect.Method;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Starts 2 brokers that are in 2 different clusters with different zookeeper
 */
@Test(groups = "broker")
@Slf4j
public class MultipleZkReplicatorTest extends MultipleZKReplicatorTestBase {

    protected String methodName;

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testReplicatorProducerCreatePartitionTopics() throws Exception {
        log.info("--- testReplicatorProducerCreatePartitionTopics ---");
        String namespace1 = "pulsar/ns";
        admin1.namespaces().createNamespace(namespace1);
        admin2.namespaces().createNamespace(namespace1);

        TopicName dest1 = TopicName.get(
                BrokerTestUtil.newUniqueName("persistent://" + namespace1 + "/testReplicatorProducerNotExceed1"));
        // TODO: the non-partition topic can not be auto-created in the remote clusters when the namespace is empty.
        admin1.topics().createPartitionedTopic(dest1.toString(), 3);
        admin2.topics().createPartitionedTopic(dest1.toString(), 3);

        dest1 = TopicName.get(
                BrokerTestUtil.newUniqueName("persistent://" + namespace1 + "/testReplicatorProducerNotExceed1"));

        admin1.topics().createPartitionedTopic(dest1.toString(), 3);
        admin1.topics().setReplicationClusters(dest1.toString(), List.of(cluster1, cluster2));

        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder().serviceUrl(url1.toString()).build();

        Producer<byte[]> producer = client.newProducer().topic(dest1.toString()).create();

        for (int i = 0; i < 10; i++) {
            producer.newMessage().send();
        }
        // TODO: the Awaitility is not necessary after fix, it can be help to reproduce the issue after reverting fix.
        //  It can be remove before merging.
        Awaitility.await().untilAsserted(() -> {
            List<String> topics1 = admin1.topics().getList(namespace1);
            List<String> partitionTopics1 = admin1.topics().getPartitionedTopicList(namespace1);

            List<String> topics2 = admin2.topics().getList(namespace1);
            List<String> partitionTopics2 = admin2.topics().getPartitionedTopicList(namespace1);

            assertEquals(topics1, topics2);
            assertEquals(partitionTopics1, partitionTopics2);

            assertNotEquals(topics1.size(), 0);
            assertNotEquals(partitionTopics1.size(), 0);
        });
    }
}
