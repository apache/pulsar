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
package org.apache.pulsar.broker.transaction;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that a broker restart recovers the transaction coordinators it owns without requiring a transaction
 * client to connect, so open transactions of crashed clients still time out and release the partitions they
 * hold.
 */
public class TransactionCoordinatorRecoveryTest extends MockedPulsarServiceBaseTest {

    private static final String NAMESPACE = "pulsar/txn-coordinator-recovery";
    private static final String TOPIC = "persistent://" + NAMESPACE + "/topic";
    private static final String SUBSCRIPTION = "sub";
    private static final long TXN_TIMEOUT_SECONDS = 20;

    private PulsarClient txnClient;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration config = getDefaultConf();
        config.setTransactionCoordinatorEnabled(true);
        config.setBrokerDeduplicationEnabled(true);
        super.internalSetup(config);

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.namespaces().createNamespace(NAMESPACE);
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(1));

        txnClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .enableTransaction(true)
                .build();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (txnClient != null) {
            txnClient.close();
            txnClient = null;
        }
        super.internalCleanup();
    }

    @Test
    public void testOpenTransactionIsAbortedAfterBrokerRestart() throws Exception {
        admin.topics().createNonPartitionedTopic(TOPIC);

        @Cleanup
        Consumer<byte[]> consumer = txnClient.newConsumer(Schema.BYTES)
                .topic(TOPIC)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        @Cleanup
        Producer<byte[]> producer = txnClient.newProducer(Schema.BYTES).topic(TOPIC).create();

        Transaction txn = txnClient.newTransaction()
                .withTransactionTimeout(TXN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .build()
                .get();
        producer.newMessage(txn).value("in-transaction".getBytes(StandardCharsets.UTF_8)).send();
        producer.newMessage().value("after-transaction".getBytes(StandardCharsets.UTF_8)).send();

        // Simulate a crashed transaction client: it leaves the transaction open and never reconnects.
        producer.close();
        consumer.close();
        txnClient.close();
        txnClient = null;

        restartBroker();

        // The coordinator must be recovered at startup without any transaction client connecting.
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(
                pulsar.getTransactionMetadataStoreService().getStores().size(), 1));

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        @Cleanup
        Consumer<byte[]> restartedConsumer = client.newConsumer(Schema.BYTES)
                .topic(TOPIC)
                .subscriptionName(SUBSCRIPTION)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        // The transaction times out after the restart. The transaction buffer then releases the partition,
        // and only the non-transactional message published after the transaction is delivered.
        Message<byte[]> message = restartedConsumer.receive((int) TXN_TIMEOUT_SECONDS + 30, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(new String(message.getValue(), StandardCharsets.UTF_8), "after-transaction");
        assertNull(restartedConsumer.receive(1, TimeUnit.SECONDS));
    }
}
