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
package org.apache.pulsar.testclient;

import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.net.URL;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The perf transaction tools target the scalable-topics (v5) coordinator, which is transaction-aware
 * only for scalable {@code topic://} topics (PIP-473). These tests therefore pre-create scalable
 * topics and verify with a v5 SDK client (the v4 SDK can't produce/consume on scalable topics). The
 * tools themselves are unchanged — they just receive {@code topic://} names of pre-created topics.
 */
@CustomLog
public class PerformanceTransactionTest extends MockedPulsarServiceBaseTest {
    private final String testTenant = "pulsar";
    private final String testNamespace = "perf";
    private final String myNamespace = testTenant + "/" + testNamespace;
    // v5 transactions are scalable-topic-only; scalable topics use the topic:// domain.
    private final String testTopic = "topic://" + myNamespace + "/test-";
    private final AtomicInteger lastExitCode = new AtomicInteger(0);

    // v5 SDK verification client: v5 transactions are scalable-topic-only, and the v4 SDK can't
    // produce/consume on scalable (topic://) topics.
    private PulsarClient v5Client;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration serviceConfiguration = getDefaultConf();
        serviceConfiguration.setTopicLevelPoliciesEnabled(false);
        serviceConfiguration.setTransactionCoordinatorEnabled(true);
        super.internalSetup(serviceConfiguration);
        PerfClientUtils.setExitProcedure(code -> {
            log.error().attr("code", code).log("JVM exit code is");
            if (code != 0) {
                throw new RuntimeException("JVM should exit with code " + code);
            }
        });
        // Setup namespaces
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(1));

        // transactionPolicy(...) opts the verification client into transactions and routes it to the
        // scalable-topics (v5) coordinator.
        v5Client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .transactionPolicy(TransactionPolicy.builder().timeout(Duration.ofMinutes(5)).build())
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
        int exitCode = lastExitCode.get();
        if (exitCode != 0) {
            fail("Unexpected JVM exit code " + exitCode);
        }
    }

    @Test
    public void testTxnPerf() throws Exception {
        String argString = "--topics-c %s --topics-p %s -threads 1 -ntxn 50 -u %s -ss %s --scalable -au %s";
        String testConsumeTopic = testTopic + UUID.randomUUID();
        String testProduceTopic = testTopic + UUID.randomUUID();
        String testSub = "testSub";
        String args = String.format(argString, testConsumeTopic, testProduceTopic,
                pulsar.getBrokerServiceUrl(), testSub, new URL(pulsar.getWebServiceAddress()));

        // Scalable topics must be pre-created (they don't auto-create on produce).
        admin.scalableTopics().createScalableTopic(testConsumeTopic, 1);
        admin.scalableTopics().createScalableTopic(testProduceTopic, 1);

        Producer<byte[]> produceToConsumeTopic = v5Client.newProducer(Schema.bytes())
                .topic(testConsumeTopic)
                .create();
        v5Client.newQueueConsumer(Schema.bytes())
                .topic(testConsumeTopic)
                .subscriptionName(testSub + "pre")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        for (int i = 0; i < 50; i++) {
            produceToConsumeTopic.newMessage().value(("testConsume " + i).getBytes()).send();
        }

        Thread thread = new Thread(() -> {
            try {
                new PerformanceTransaction().run(args.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        thread.join();

        QueueConsumer<byte[]> consumeFromConsumeTopic = v5Client.newQueueConsumer(Schema.bytes())
                .topic(testConsumeTopic)
                .subscriptionName(testSub)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        QueueConsumer<byte[]> consumeFromProduceTopic = v5Client.newQueueConsumer(Schema.bytes())
                .topic(testProduceTopic)
                .subscriptionName(testSub)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        for (int i = 0; i < 50; i++) {
            Message<byte[]> message = consumeFromProduceTopic.receive(Duration.ofSeconds(10));
            Assert.assertNotNull(message);
            consumeFromProduceTopic.acknowledge(message.id());
        }
        Message<byte[]> message = consumeFromConsumeTopic.receive(Duration.ofSeconds(2));
        Assert.assertNull(message);
        message = consumeFromProduceTopic.receive(Duration.ofSeconds(2));
        Assert.assertNull(message);
    }

    @Test
    public void testProduceTxnMessage() throws Exception {
        String argString = "%s -r 50 -u %s -m %d -txn";
        String topic = testTopic + UUID.randomUUID();
        int totalMessage = 100;
        String args = String.format(argString, topic, pulsar.getBrokerServiceUrl(), totalMessage);

        admin.scalableTopics().createScalableTopic(topic, 1);

        @Cleanup
        QueueConsumer<byte[]> subscribe = v5Client.newQueueConsumer(Schema.bytes())
                .subscriptionName("subName" + "pre")
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        Thread thread = new Thread(() -> {
            try {
                new PerformanceProducer().run(args.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        thread.join();

        @Cleanup
        QueueConsumer<byte[]> consumer = v5Client.newQueueConsumer(Schema.bytes())
                .subscriptionName("subName")
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        // PerformanceProducer commits its transactions asynchronously and run() returns without
        // awaiting them, so committed messages may still be becoming visible after the join. Drain
        // up to a deadline rather than assuming all are immediately readable.
        int received = 0;
        long deadline = System.currentTimeMillis() + 30_000;
        while (received < totalMessage && System.currentTimeMillis() < deadline) {
            Message<byte[]> message = consumer.receive(Duration.ofSeconds(2));
            if (message == null) {
                continue;
            }
            consumer.acknowledge(message.id());
            received++;
        }
        Assert.assertEquals(received, totalMessage, "all committed produced messages must be delivered");
        Message<byte[]> message = consumer.receive(Duration.ofSeconds(2));
        Assert.assertNull(message);
    }

    @Test
    public void testConsumeTxnMessage() throws Exception {
        // A long transaction timeout (-tto) so none of the consumer's transactions time out and abort
        // on the slower scalable-topic path: an aborted txn would release its pending-acked messages
        // for redelivery and inflate what the verifier sees below.
        String argString = "%s -r 50 -u %s -txn -ss %s -st %s -sp %s -ntxn %d -tto 60";
        String subName = "sub";
        String topic = testTopic + UUID.randomUUID();
        // -st is PerformanceConsumer's own SubscriptionType enum (Exclusive); -sp is the v5
        // SubscriptionInitialPosition enum (EARLIEST).
        String args = String.format(argString, topic, pulsar.getBrokerServiceUrl(), subName,
                "Exclusive", "EARLIEST", 10);

        admin.scalableTopics().createScalableTopic(topic, 1);

        @Cleanup
        Producer<byte[]> producer = v5Client.newProducer(Schema.bytes()).topic(topic).create();
        @Cleanup
        QueueConsumer<byte[]> subscribe = v5Client.newQueueConsumer(Schema.bytes())
                .topic(topic)
                .subscriptionName(subName + "pre")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        // Exactly numMessagesPerTransaction (50) * -ntxn (10) messages, so the perf consumer commits
        // all of them across 10 transactions and leaves the subscription empty.
        for (int i = 0; i < 500; i++) {
            producer.newMessage().value("messages for test transaction consumer".getBytes()).send();
        }
        Thread thread = new Thread(() -> {
            try {
                new PerformanceConsumer().run(args.split(" "));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "Performance_Consumer_Task");
        thread.start();
        thread.join();

        // The perf consumer committed all 10 txns * 50 msgs = 500 transactional acks, so every message
        // is permanently acknowledged and a fresh consumer on the same subscription sees nothing.
        @Cleanup
        QueueConsumer<byte[]> consumer = v5Client.newQueueConsumer(Schema.bytes())
                .subscriptionName(subName)
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        Message<byte[]> message = consumer.receive(Duration.ofSeconds(2));
        Assert.assertNull(message, "all transactionally-acked messages must stay acknowledged");
    }

}
