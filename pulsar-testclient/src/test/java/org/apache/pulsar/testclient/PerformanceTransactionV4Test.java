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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
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
 * End-to-end coverage of {@code pulsar-perf transaction-v4}, which drives the v4 client and the v4
 * transaction coordinator against ordinary {@code persistent://} topics. The V5 counterpart lives in
 * {@link PerformanceTransactionTest}, which needs scalable topics and a V5 SDK client; here the v4
 * SDK client the base test already provides is the verifier.
 */
@CustomLog
public class PerformanceTransactionV4Test extends MockedPulsarServiceBaseTest {

    private static final int PUBLISHED = 10;
    private static final int TRANSACTIONS = 5;
    private static final Duration RUN_TIMEOUT = Duration.ofSeconds(90);

    private final String myNamespace = "pulsar/perf";
    private final String testTopic = "persistent://" + myNamespace + "/test-";
    private final AtomicInteger lastExitCode = new AtomicInteger(0);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration serviceConfiguration = getDefaultConf();
        serviceConfiguration.setTopicLevelPoliciesEnabled(false);
        serviceConfiguration.setTransactionCoordinatorEnabled(true);
        super.internalSetup(serviceConfiguration);
        lastExitCode.set(0);
        PerfClientUtils.setExitProcedure(code -> {
            log.info().attr("code", code).log("Perf tool requested JVM exit");
            lastExitCode.set(code);
        });
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        // SYSTEM_NAMESPACE's tenant is "pulsar", the same tenant myNamespace lives under.
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(myNamespace, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(1));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        assertThat(lastExitCode.get()).as("perf tool JVM exit code").isZero();
    }

    /**
     * A committed run must make both halves of its unit of work durable. This fails if the client is
     * not built with {@code enableTransaction}, or if opening or committing the transaction is
     * broken: in each case nothing is ever committed to the produce topic.
     *
     * <p>The acknowledgement assertion is what stops a run that never acknowledged at all from
     * satisfying {@link #testTransactionV4AbortUndoesBothTheSendAndTheAck()}'s "backlog unchanged".
     */
    @Test(timeOut = 120000)
    public void testTransactionV4CommitsWhatItProduced() throws Exception {
        String consumeTopic = testTopic + UUID.randomUUID();
        String produceTopic = testTopic + UUID.randomUUID();
        String subscription = "sub-" + UUID.randomUUID();
        publish(consumeTopic, PUBLISHED);

        run("--topics-c %s --topics-p %s -threads 1 -ntxn %d -tto 60 -u %s -ss %s",
                consumeTopic, produceTopic, TRANSACTIONS, pulsar.getBrokerServiceUrl(), subscription);

        @Cleanup
        Consumer<byte[]> produced = verifier(produceTopic);
        for (int i = 0; i < TRANSACTIONS; i++) {
            assertThat(produced.receive(30, TimeUnit.SECONDS))
                    .as("message %d committed by transaction-v4", i)
                    .isNotNull();
        }

        // One message is consumed and acknowledged per transaction, and committing makes those acks
        // durable, so the backlog drops by exactly the number of transactions.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(admin.topics().getStats(consumeTopic).getSubscriptions()
                                .get(subscription).getMsgBacklog())
                        .as("acknowledgements committed by transaction-v4")
                        .isEqualTo(PUBLISHED - TRANSACTIONS));
    }

    /**
     * {@code -abort} must undo both halves of the unit of work. A send that was not actually bound to
     * the transaction would still become visible, and an ack that was not bound to it would still be
     * permanent, so this is what pins the {@code transaction != null} branches of
     * {@code PerformanceTransactionV4.sendMessage} and {@code acknowledgeAsync} onto the transaction.
     */
    @Test(timeOut = 120000)
    public void testTransactionV4AbortUndoesBothTheSendAndTheAck() throws Exception {
        String consumeTopic = testTopic + UUID.randomUUID();
        String produceTopic = testTopic + UUID.randomUUID();
        String subscription = "sub-" + UUID.randomUUID();
        publish(consumeTopic, PUBLISHED);

        run("--topics-c %s --topics-p %s -threads 1 -ntxn %d -tto 60 -u %s -ss %s -abort",
                consumeTopic, produceTopic, TRANSACTIONS, pulsar.getBrokerServiceUrl(), subscription);

        @Cleanup
        Consumer<byte[]> produced = verifier(produceTopic);
        assertThat(produced.receive(5, TimeUnit.SECONDS))
                .as("a message sent under an aborted transaction must never become visible")
                .isNull();
        assertThat(admin.topics().getStats(consumeTopic).getSubscriptions()
                        .get(subscription).getMsgBacklog())
                .as("an ack made under an aborted transaction must not stay acknowledged")
                .isEqualTo(PUBLISHED);

        // Aborting *releases* the acknowledgements, so all ten messages are redeliverable on the
        // tool's own subscription right away. A transaction that was merely never ended would leave
        // the five it acknowledged pending until the transaction timeout, and only five would
        // arrive — which is what separates a real abort from a no-op one.
        @Cleanup
        Consumer<byte[]> redelivered = pulsarClient.newConsumer().topic(consumeTopic)
                .subscriptionName(subscription).subscribe();
        for (int i = 0; i < PUBLISHED; i++) {
            assertThat(redelivered.receive(15, TimeUnit.SECONDS))
                    .as("message %d released by the abort", i)
                    .isNotNull();
        }
    }

    /**
     * Runs the command to completion; the overridden exit procedure records the code rather than
     * exiting the JVM, and {@link #cleanup()} asserts it was zero. The join is bounded and the
     * thread is interrupted if it overruns, so a hung command cannot outlive the test and keep
     * running against a broker that is being torn down.
     */
    private void run(String argFormat, Object... argValues) throws InterruptedException {
        String[] args = String.format(argFormat, argValues).split(" ");
        AtomicBoolean succeeded = new AtomicBoolean();
        Thread thread = new Thread(() -> {
            try {
                succeeded.set(new PerformanceTransactionV4().run(args));
            } catch (Exception e) {
                log.error().exception(e).log("transaction-v4 failed");
            }
        }, "transaction-v4");
        thread.start();
        thread.join(RUN_TIMEOUT.toMillis());
        if (thread.isAlive()) {
            thread.interrupt();
            thread.join(Duration.ofSeconds(10).toMillis());
            fail("transaction-v4 did not finish within " + RUN_TIMEOUT);
        }
        assertThat(succeeded.get()).as("transaction-v4 exited cleanly").isTrue();
    }

    private Consumer<byte[]> verifier(String topic) throws Exception {
        return pulsarClient.newConsumer().topic(topic).subscriptionName("verify")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
    }

    /**
     * Batching is off on purpose: acknowledging part of a batch does not move the mark-delete
     * position unless batch-index acknowledgement is enabled, which would make the backlog assertion
     * pass whether or not the acks were transactional.
     */
    private void publish(String topic, int numMessages) throws Exception {
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        for (int i = 0; i < numMessages; i++) {
            producer.send(("message-" + i).getBytes());
        }
    }
}
