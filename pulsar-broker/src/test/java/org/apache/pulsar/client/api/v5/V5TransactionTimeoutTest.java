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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
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
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Diagnostic for the v5 transaction timeout sweep: a never-committed transaction must be aborted by
 * the broker's timeout sweep, unpinning the buffer so a later non-transactional message becomes
 * visible.
 */
@CustomLog
public class V5TransactionTimeoutTest extends MockedPulsarServiceBaseTest {

    private final String myNamespace = "pulsar/txn-timeout";
    private PulsarClient v5Client;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration config = getDefaultConf();
        config.setTransactionCoordinatorEnabled(true);
        config.setTopicLevelPoliciesEnabled(false);
        config.setTransactionCoordinatorScalableTopicsTimeoutSweepIntervalSeconds(1);
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
                .transactionPolicy(TransactionPolicy.builder().timeout(Duration.ofSeconds(3)).build())
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
    public void testTimeoutSweepAbortsDanglingTransaction() throws Exception {
        // Stage 1: confirm this broker actually leads TC partition 0 (the sweep only runs there).
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() ->
                pulsar.getTransactionCoordinatorV5() != null
                        && pulsar.getTransactionCoordinatorV5().isLeaderFor(0));
        log.info().log("Stage 1 OK: broker leads TC partition 0");

        String topic = "topic://" + myNamespace + "/scalable-" + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(topic, 1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string()).topic(topic).create();
        Transaction txn = v5Client.newTransaction();
        for (int i = 0; i < 5; i++) {
            producer.newMessage().transaction(txn).value("dangling-" + i).send();
        }
        producer.newMessage().value("sentinel").send();

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("timeout-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        assertNull(consumer.receive(Duration.ofSeconds(1)), "Stage 2: everything pinned while txn open");
        log.info().log("Stage 2 OK: pinned while txn open");

        // Stage 3: the timeout sweep (1s cadence) aborts the dangling txn after its 3s timeout.
        Message<String> msg = null;
        long deadline = System.currentTimeMillis() + 40_000;
        while (msg == null && System.currentTimeMillis() < deadline) {
            msg = consumer.receive(Duration.ofSeconds(2));
        }
        assertNotNull(msg, "Stage 3: sentinel must appear once the timeout sweep aborts the dangling txn");
        org.testng.Assert.assertEquals(msg.value(), "sentinel");
        consumer.acknowledge(msg.id());
        log.info().log("Stage 3 OK: timeout sweep unpinned the buffer");
    }
}
