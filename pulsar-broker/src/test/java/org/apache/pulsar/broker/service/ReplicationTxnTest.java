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

import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.reflect.WhiteboxImpl;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ReplicationTxnTest extends OneWayReplicatorTestBase {

    private boolean transactionBufferSegmentedSnapshotEnabled = false;

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

    @Override
    protected PulsarClient initClient(ClientBuilder clientBuilder) throws Exception {
        return clientBuilder.enableTransaction(true).build();
    }

    @Override
    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        config.setSystemTopicEnabled(true);
        config.setTopicLevelPoliciesEnabled(true);
        config.setTransactionCoordinatorEnabled(true);
        config.setTransactionLogBatchedWriteEnabled(true);
        config.setTransactionPendingAckBatchedWriteEnabled(true);
        config.setTransactionBufferSegmentedSnapshotEnabled(transactionBufferSegmentedSnapshotEnabled);
    }

    @Override
    protected void createDefaultTenantsAndClustersAndNamespace() throws Exception {
        super.createDefaultTenantsAndClustersAndNamespace();

        // Create resource that transaction function relies on.
        admin1.tenants().createTenant(SYSTEM_NAMESPACE.getTenant(), new TenantInfoImpl(Collections.emptySet(),
                Sets.newHashSet(cluster1, cluster2)));
        admin1.namespaces().createNamespace(SYSTEM_NAMESPACE.toString(), 4);
        pulsar1.getPulsarResources().getNamespaceResources().getPartitionedTopicResources().createPartitionedTopic(
                SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN, new PartitionedTopicMetadata(4));
        //admin1.topics().createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.toString(), 4);

        admin2.tenants().createTenant(SYSTEM_NAMESPACE.getTenant(), new TenantInfoImpl(Collections.emptySet(),
                Sets.newHashSet(cluster1, cluster2)));
        admin2.namespaces().createNamespace(SYSTEM_NAMESPACE.toString(), 4);
        pulsar2.getPulsarResources().getNamespaceResources().getPartitionedTopicResources().createPartitionedTopic(
                SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN, new PartitionedTopicMetadata(4));
    }

    @Test
    public void testSendMessage() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp");
        final String subscription = "s1";
        admin1.topics().createNonPartitionedTopic(topic);
        waitReplicatorStarted(topic);
        admin2.topics().createSubscription(topic, subscription, MessageId.earliest);

        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topic).create();
        Transaction txn = client1.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
        producer1.newMessage(txn).value("txn_msg_1").send();
        txn.commit().get();
        producer1.close();

        Consumer consumer2 = client2.newConsumer(Schema.STRING).topic(topic).subscriptionName(subscription).subscribe();
        Awaitility.await().untilAsserted(() -> {
            Message<String> msg = consumer2.receive(2, TimeUnit.SECONDS);
            assertNotNull(msg);
            assertEquals(msg.getValue(), "txn_msg_1");
            MessageMetadata msgMetadata2 = WhiteboxImpl.getInternalState(msg, "msgMetadata");
            assertFalse(msgMetadata2.hasTxnidMostBits());
            assertFalse(msgMetadata2.hasTxnidLeastBits());
        });

        // cleanup.
        consumer2.close();
        cleanupTopics(() -> {
            admin1.topics().delete(topic);
            admin2.topics().delete(topic);
        });
    }
}
