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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class OneWayReplicatorTest extends OneWayReplicatorTestBase {

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
    public void testReplicatorProducerStatInTopic() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
        final String subscribeName = "subscribe_1";
        final byte[] msgValue = "test".getBytes();

        admin1.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, subscribeName, MessageId.earliest);
        admin2.topics().createSubscription(topicName, subscribeName, MessageId.earliest);

        // Verify replicator works.
        Producer<byte[]> producer1 = client1.newProducer().topic(topicName).create();
        Consumer<byte[]> consumer2 = client2.newConsumer().topic(topicName).subscriptionName(subscribeName).subscribe();
        producer1.newMessage().value(msgValue).send();
        pulsar1.getBrokerService().checkReplicationPolicies();
        assertEquals(consumer2.receive(10, TimeUnit.SECONDS).getValue(), msgValue);

        // Verify there has one item in the attribute "publishers" or "replications"
        TopicStats topicStats2 = admin2.topics().getStats(topicName);
        assertTrue(topicStats2.getPublishers().size() + topicStats2.getReplication().size() > 0);

        // cleanup.
        consumer2.close();
        producer1.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }

    @Test
    public void testCreateRemoteConsumerFirst() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + defaultNamespace + "/tp_");
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topicName).create();
        // Wait for replicator started.
        Awaitility.await().untilAsserted(() -> {
            Optional<Topic> topicOptional2 = pulsar2.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional2.isPresent());
            PersistentTopic persistentTopic2 = (PersistentTopic) topicOptional2.get();
            assertFalse(persistentTopic2.getProducers().isEmpty());
        });
        // The topic in cluster2 has a replicator created producer(schema Auto_Produce), but does not have any schema。
        // Verify: the consumer of this cluster2 can create successfully.
        Consumer<String> consumer2 = client2.newConsumer(Schema.STRING).topic(topicName).subscriptionName("s1")
                .subscribe();;

        // cleanup.
        producer1.close();
        consumer2.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topicName);
            admin2.topics().delete(topicName);
        });
    }
}
