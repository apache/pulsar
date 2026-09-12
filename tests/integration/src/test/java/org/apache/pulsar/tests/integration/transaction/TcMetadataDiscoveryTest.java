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
package org.apache.pulsar.tests.integration.transaction;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.testng.annotations.Test;

/**
 * Integration test for transaction-coordinator coexistence (PIP-473): on a cluster with the
 * scalable-topics (v5) coordinator enabled, a v5 SDK client routes its transactions to the
 * metadata-store coordinator while a v4 SDK client keeps using the legacy coordinator — both on the
 * same cluster. Routing is decided by client/SDK kind (the v5 SDK sets an internal flag), not by
 * broker capability, so flipping the broker default to enable v5 must not break v4 transactions.
 *
 * <p>Scope: transaction-coordinator routing + the v5 lifecycle/failover. Full v5 data-in-transaction
 * (produce/ack on segment topics) is exercised separately.
 */
@CustomLog
public class TcMetadataDiscoveryTest extends TcMetadataDiscoveryTestBase {

    /**
     * A v5 SDK client runs many transactions (commit and abort) against the v5-enabled cluster. This
     * only succeeds if the client routed to the running metadata-store coordinator — a regression
     * that broke the watch path or mis-routed v5 to the legacy TC would fail here.
     */
    @Test
    public void v5SdkTransactionsUseMetadataCoordinator() throws Exception {
        @Cleanup
        org.apache.pulsar.client.api.v5.PulsarClient client =
                org.apache.pulsar.client.api.v5.PulsarClient.builder()
                        .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                        .transactionPolicy(org.apache.pulsar.client.api.v5.config.TransactionPolicy.builder()
                                .timeout(java.time.Duration.ofMinutes(1)).build())
                        .build();

        // Enough transactions that the client's round-robin visits every coordinator partition; if
        // any were mis-routed to the legacy TC (which doesn't coordinate scalable transactions) or
        // the watch path were broken, these would fail.
        runV5Transactions(client, TC_PARALLELISM * 4);
    }

    /**
     * Kill one broker and confirm the v5 SDK client keeps working: coordinator partitions led by the
     * dead broker are re-elected to the survivor, the client's assignment watch retargets, and
     * subsequent transactions still succeed.
     */
    @Test
    public void v5SdkTransactionsSurviveLeaderBrokerFailure() throws Exception {
        @Cleanup
        org.apache.pulsar.client.api.v5.PulsarClient client =
                org.apache.pulsar.client.api.v5.PulsarClient.builder()
                        .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                        .transactionPolicy(org.apache.pulsar.client.api.v5.config.TransactionPolicy.builder()
                                .timeout(java.time.Duration.ofMinutes(1)).build())
                        .build();

        runV5Transactions(client, TC_PARALLELISM * 4);

        BrokerContainer victim = pulsarCluster.getBrokers().iterator().next();
        log.info().attr("broker", victim.getContainerName()).log("Stopping broker to force TC failover");
        victim.stop();

        // After re-election + assignment-watch refresh, transactions succeed again. runV5Transactions
        // retries within a bounded wait while leadership and the client's handlers converge.
        runV5Transactions(client, TC_PARALLELISM * 4);
    }

    /**
     * Run {@code count} v5 transactions (commit) back to back. Each transaction that fails with a
     * transient error (a coordinator still connecting, or mid-reconnect after a failover) is retried
     * up to a deadline rather than spacing one transaction per poll interval — driving them in a
     * tight loop keeps total wall-clock bounded by transaction latency, not by the retry cadence.
     */
    private void runV5Transactions(org.apache.pulsar.client.api.v5.PulsarClient client, int count)
            throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(3);
        for (int i = 0; i < count; i++) {
            while (true) {
                try {
                    org.apache.pulsar.client.api.v5.Transaction txn = client.newTransaction();
                    txn.commit();
                    break;
                } catch (Exception e) {
                    if (System.currentTimeMillis() > deadline) {
                        throw e;
                    }
                    Thread.sleep(500);
                }
            }
        }
    }

    /**
     * Coexistence: with the v5 coordinator enabled on the cluster, a v4 SDK client running a
     * transaction on a {@code persistent://} topic must still use the legacy coordinator and work end
     * to end. Routing by client kind means the v4 client's commands carry no {@code scalable} flag,
     * so the broker sends them to the legacy TC. This is the regression guard for the P5.4 default
     * flip.
     */
    @Test
    public void v4SdkTransactionStillUsesLegacyCoordinator() throws Exception {
        @Cleanup
        org.apache.pulsar.client.api.PulsarClient client =
                org.apache.pulsar.client.api.PulsarClient.builder()
                        .enableTransaction(true)
                        .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                        .build();

        // Routing assertion: a v4 SDK client must NOT use metadata-store discovery even though the
        // cluster has the v5 coordinator enabled — it stays on the legacy assign-topic coordinator.
        TransactionCoordinatorClientImpl tcClient = ((PulsarClientImpl) client).getTcClient();
        assertFalse(tcClient.isUsingMetadataDiscovery(),
                "v4 SDK client must use the legacy coordinator, not metadata-store discovery");

        String topic = "persistent://public/default/v4-coexist-" + randomName(6);

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING).topic(topic).create();
        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("coexist-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        // Non-transactional produce, then a transactional ack — exercises the legacy TC end to end
        // (newTxn -> addSubscription -> endTxn) on a persistent topic.
        producer.send("m1");
        Message<String> msg = consumer.receive(15, TimeUnit.SECONDS);
        assertNotNull(msg, "should receive the produced message");

        Transaction txn = client.newTransaction()
                .withTransactionTimeout(1, TimeUnit.MINUTES)
                .build().get();
        consumer.acknowledgeAsync(msg.getMessageId(), txn).get();
        txn.commit().get();

        // After commit the message is acknowledged: redelivery on reconnect must not return it.
        consumer.redeliverUnacknowledgedMessages();
        assertNull(consumer.receive(5, TimeUnit.SECONDS),
                "committed transactional ack should have consumed the message");
    }
}
