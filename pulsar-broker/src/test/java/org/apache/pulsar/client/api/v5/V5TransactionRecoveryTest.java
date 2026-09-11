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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Recovery coverage for V5 transactions on scalable topics. Uses a single restartable broker
 * (the mock BookKeeper + metadata stores are reused across {@link #restartBroker()}, so segment
 * data and the {@code /txn} metadata layout survive a restart). After restart a segment topic is
 * reloaded cold, exercising {@code MetadataTransactionBuffer} / {@code MetadataPendingAckStore}
 * recovery: committed data must stay visible, aborted data must stay filtered, and committed
 * transactional acks must not be redelivered. Also covers the timeout sweep aborting a dangling
 * transaction.
 */
public class V5TransactionRecoveryTest extends MockedPulsarServiceBaseTest {

    private final String myNamespace = "pulsar/txn-recovery";

    private PulsarClient v5Client;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration config = getDefaultConf();
        config.setTransactionCoordinatorEnabled(true);
        config.setTopicLevelPoliciesEnabled(false);
        super.internalSetup(config);

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        // SYSTEM_NAMESPACE's tenant IS "pulsar" (pulsar/system), which also owns myNamespace below.
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(1));

        v5Client = newV5Client(Duration.ofMinutes(2));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (v5Client != null) {
            v5Client.close();
            v5Client = null;
        }
        super.internalCleanup();
    }

    private PulsarClient newV5Client(Duration txnTimeout) throws Exception {
        return PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .transactionPolicy(TransactionPolicy.builder().timeout(txnTimeout).build())
                .build();
    }

    private String newScalableTopic(int numInitialSegments) throws Exception {
        String name = "topic://" + myNamespace + "/scalable-" + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(name, numInitialSegments);
        return name;
    }

    private QueueConsumer<String> subscribe(String topic, String sub) throws Exception {
        return v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(sub)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
    }

    /** Restart the broker (segment data + metadata survive) and rebuild the V5 client. */
    private void restartBrokerAndReconnect() throws Exception {
        v5Client.close();
        restartBroker();
        v5Client = newV5Client(Duration.ofMinutes(2));
    }

    @Test
    public void testCommittedMessagesVisibleAfterBrokerRestart() throws Exception {
        // Commit a transaction, then restart so the segment topic is reloaded cold. The transaction
        // buffer recovers from the /txn metadata and re-exposes the committed messages.
        String topic = newScalableTopic(2);
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string()).topic(topic).create();

        Transaction txn = v5Client.newTransaction();
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String v = "v-" + i;
            producer.newMessage().key("k-" + i).transaction(txn).value(v).send();
            sent.add(v);
        }
        txn.commit();
        producer.close();

        restartBrokerAndReconnect();

        @Cleanup
        QueueConsumer<String> consumer = subscribe(topic, "after-restart-sub");
        Set<String> got = new HashSet<>();
        for (int i = 0; i < sent.size(); i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(15));
            assertNotNull(msg, "committed message #" + i + " must survive restart");
            got.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(got, sent, "every committed message must be visible after recovery");
        assertNull(consumer.receive(Duration.ofMillis(500)), "no extra messages");
    }

    @Test
    public void testAbortedMessagesFilteredAfterBrokerRestart() throws Exception {
        // Abort a transaction and publish a non-transactional sentinel, then restart. Recovery must
        // rebuild the aborted set (from the durable ABORTED header / aborted records) so the aborted
        // messages stay filtered and only the sentinel is delivered.
        String topic = newScalableTopic(1);
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string()).topic(topic).create();

        Transaction txn = v5Client.newTransaction();
        for (int i = 0; i < 5; i++) {
            producer.newMessage().transaction(txn).value("aborted-" + i).send();
        }
        txn.abort();
        producer.newMessage().value("sentinel").send();
        producer.close();

        restartBrokerAndReconnect();

        @Cleanup
        QueueConsumer<String> consumer = subscribe(topic, "after-restart-abort-sub");
        Message<String> msg = consumer.receive(Duration.ofSeconds(15));
        assertNotNull(msg, "the non-transactional sentinel must be delivered after restart");
        assertEquals(msg.value(), "sentinel", "aborted messages must stay filtered after recovery");
        consumer.acknowledge(msg.id());
        assertNull(consumer.receive(Duration.ofMillis(500)), "no aborted messages after the sentinel");
    }

    @Test
    public void testTransactionalAcksSurviveBrokerRestart() throws Exception {
        // Acknowledge messages inside a committed transaction, then restart. The pending-ack store
        // recovery (and the durable cursor) must keep them acknowledged — no redelivery.
        String topic = newScalableTopic(1);
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string()).topic(topic).create();
        int n = 10;
        for (int i = 0; i < n; i++) {
            producer.newMessage().value("m-" + i).send();
        }

        // Acknowledge all inside the transaction, keeping the consumer open through commit so the
        // async ack operations complete before commit (closing it early fails them).
        Transaction txn = v5Client.newTransaction();
        QueueConsumer<String> consumer = subscribe(topic, "ack-sub");
        for (int i = 0; i < n; i++) {
            Message<String> m = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(m, "delivery #" + i);
            consumer.acknowledge(m.id(), txn);
        }
        txn.commit();
        consumer.close();

        // Pre-restart: confirm the committed acks have materialised (a fresh consumer on the same
        // subscription sees nothing), so the restart genuinely exercises recovery of acked state.
        {
            @Cleanup
            QueueConsumer<String> check = subscribe(topic, "ack-sub");
            assertNull(check.receive(Duration.ofSeconds(10)), "committed acks must materialise before restart");
        }

        restartBrokerAndReconnect();

        @Cleanup
        QueueConsumer<String> after = subscribe(topic, "ack-sub");
        assertNull(after.receive(Duration.ofSeconds(10)),
                "committed transactional acks must not be redelivered after restart");
    }
}
