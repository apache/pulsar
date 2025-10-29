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

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = "broker-replication")
public class TransactionalReplicateSubscriptionTest extends ReplicatorTestBase {
    @Override
    @BeforeClass(timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
        admin1.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        createTransactionCoordinatorAssign(16, pulsar1);
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    /**
     * enable transaction coordinator for the cluster1
     */
    @Override
    public void setConfig1DefaultValue(){
        super.setConfig1DefaultValue();
        config1.setTransactionCoordinatorEnabled(true);
    }

    protected void createTransactionCoordinatorAssign(int numPartitionsOfTC, PulsarService pulsarService) throws MetadataStoreException {
        pulsarService.getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(numPartitionsOfTC));
    }

    /**
     * Test replicated subscription with transaction.
     * @throws Exception
     */
    @Test
    public void testReplicatedSubscribeAndSwitchToStandbyClusterWithTransaction() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/ns_");
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/tp_");
        final String subscriptionName = "s1";
        final boolean isReplicatedSubscription = true;
        final int messagesCount = 20;
        final LinkedHashSet<String> sentMessages = new LinkedHashSet<>();
        final Set<String> receivedMessages = Collections.synchronizedSet(new LinkedHashSet<>());
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2"));
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, subscriptionName, MessageId.earliest, isReplicatedSubscription);
        final PersistentTopic topic1 =
                (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();

        // Send messages
        // Wait for the topic created on the cluster2.
        // Wait for the snapshot created.
        final PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).enableTransaction(true).build();
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topicName).enableBatching(false).create();
        Consumer<String> consumer1 = client1.newConsumer(Schema.STRING).topic(topicName)
                .subscriptionName(subscriptionName).replicateSubscriptionState(isReplicatedSubscription).subscribe();
        Transaction txn1 = client1.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        for (int i = 0; i < messagesCount / 2; i++) {
            String msg = i + "";
            producer1.newMessage(txn1).value(msg).send();
            sentMessages.add(msg);
        }
        txn1.commit().get();
        Awaitility.await().untilAsserted(() -> {
            ConcurrentOpenHashMap<String, ? extends Replicator> replicators = topic1.getReplicators();
            assertTrue(replicators != null && replicators.size() == 1, "Replicator should started");
            assertTrue(replicators.values().iterator().next().isConnected(), "Replicator should be connected");
            assertTrue(topic1.getReplicatedSubscriptionController().get().getLastCompletedSnapshotId().isPresent(),
                    "One snapshot should be finished");
        });
        final PersistentTopic topic2 =
                (PersistentTopic) pulsar2.getBrokerService().getTopic(topicName, false).join().get();
        Awaitility.await().untilAsserted(() -> {
            assertTrue(topic2.getReplicatedSubscriptionController().isPresent(),
                    "Replicated subscription controller should created");
        });
        Transaction txn2 = client1.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();
        for (int i = messagesCount / 2; i < messagesCount; i++) {
            String msg = i + "";
            producer1.newMessage(txn2).value(msg).send();
            sentMessages.add(msg);
        }
        txn2.commit().get();

        // Consume half messages and wait the subscription created on the cluster2.
        for (int i = 0; i < messagesCount / 2; i++){
            Message<String> message = consumer1.receive(2, TimeUnit.SECONDS);
            if (message == null) {
                fail("Should not receive null.");
            }
            receivedMessages.add(message.getValue());
            consumer1.acknowledge(message);
        }
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(topic2.getSubscriptions().get(subscriptionName), "Subscription should created");
        });

        // Switch client to cluster2.
        // Since the cluster1 was not crash, all messages will be replicated to the cluster2.
        consumer1.close();
        final PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).build();
        final Consumer consumer2 = client2.newConsumer(Schema.AUTO_CONSUME()).topic(topicName)
                .subscriptionName(subscriptionName).replicateSubscriptionState(isReplicatedSubscription).subscribe();

        // Verify all messages will be consumed.
        Awaitility.await().untilAsserted(() -> {
            while (true) {
                Message message = consumer2.receive(2, TimeUnit.SECONDS);
                if (message != null) {
                    receivedMessages.add(message.getValue().toString());
                    consumer2.acknowledge(message);
                } else {
                    break;
                }
            }
            assertEquals(receivedMessages.size(), sentMessages.size());
        });

        consumer2.close();
        producer1.close();
        client1.close();
        client2.close();
    }
}
