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

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.zookeeper.MockZooKeeper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A producer mixing transactional and plain messages on a scalable topic: the broker writes the {@code /txn/op}
 * record of a transactional message before its entry, and the messages must still be stored, and acknowledged, in
 * the order they were sent. A producer acknowledged out of order drops its connection and re-sends its pending
 * messages. Uses a dedicated broker to slow down the metadata store.
 */
public class V5TransactionPublishOrderTest extends MockedPulsarServiceBaseTest {

    private final String myNamespace = "pulsar/txn-publish-order";
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
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(1));

        v5Client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .transactionPolicy(TransactionPolicy.builder().timeout(Duration.ofMinutes(1)).build())
                .build();
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

    @Test
    public void testPlainMessageSentAfterTransactionalMessageIsStoredAfterIt() throws Exception {
        String topic = "topic://" + myNamespace + "/scalable-" + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(topic, 1);
        // Without batching, each message is an entry of its own, published and acknowledged on its own.
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .create();
        @Cleanup
        StreamConsumer<String> consumer = v5Client.newStreamConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        Transaction txn = v5Client.newTransaction();
        // The first message of a transaction to a segment waits for the segment to be registered with the
        // transaction before it's sent, so a message sent right after it would go first.
        producer.newMessage().transaction(txn).value("txn-0").send();

        // Hold back the op record of the next transactional message, as a slow metadata store would: the plain
        // message sent after it reaches the broker while the transactional one still waits for its op record.
        mockZooKeeper.delay(500, (op, path) ->
                op == MockZooKeeper.Op.CREATE && path.startsWith(TxnPaths.TXN_OP_PREFIX + "/"));
        CompletableFuture<MessageId> txnSend = producer.async().newMessage().transaction(txn).value("txn-1").send();
        CompletableFuture<MessageId> plainSend = producer.async().newMessage().value("plain").send();
        assertThat(txnSend.get(10, TimeUnit.SECONDS)).isLessThan(plainSend.get(10, TimeUnit.SECONDS));
        txn.commit();

        // Each message is stored once, in the order it was sent.
        List<String> received = new ArrayList<>();
        for (Message<String> msg = consumer.receive(Duration.ofSeconds(10)); msg != null;
                msg = consumer.receive(Duration.ofSeconds(1))) {
            received.add(msg.value());
        }
        assertThat(received).containsExactly("txn-0", "txn-1", "plain");
    }
}
