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
package org.apache.pulsar.broker.transaction.buffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.google.common.collect.Sets;

import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Pulsar client transaction test.
 */
@Slf4j
@Test(groups = "broker")
public class TransactionStablePositionTest extends TransactionTestBase {

    private static final String TENANT = "tnx";
    private static final String NAMESPACE1 = TENANT + "/ns1";
    private static final String TOPIC = NAMESPACE1 + "/test-topic";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createNonPartitionedTopic(TOPIC);
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
        pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();

        Awaitility.await().until(() -> ((PulsarClientImpl) pulsarClient)
                .getTcClient().getState() == TransactionCoordinatorClient.State.READY);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void commitTxnTest() throws Exception {
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        final String TEST1 = "test1";
        final String TEST2 = "test2";
        final String TEST3 = "test3";

        producer.newMessage().value(TEST1.getBytes()).send();
        producer.newMessage(txn).value(TEST2.getBytes()).send();
        producer.newMessage().value(TEST3.getBytes()).send();

        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST1);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);

        txn.commit().get();

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST2);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST3);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);
    }

    @Test
    public void abortTxnTest() throws Exception {
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(TOPIC)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .enableBatchIndexAcknowledgment(true)
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();
        final String TEST1 = "test1";
        final String TEST2 = "test2";
        final String TEST3 = "test3";

        producer.newMessage().value(TEST1.getBytes()).send();
        producer.newMessage(txn).value(TEST2.getBytes()).send();
        producer.newMessage().value(TEST3.getBytes()).send();

        Message<byte[]> message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST1);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);

        txn.abort().get();

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertEquals(new String(message.getData()), TEST3);

        message = consumer.receive(2, TimeUnit.SECONDS);
        assertNull(message);
    }

}
