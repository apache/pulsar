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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Integration test for the metadata-store transaction-coordinator discovery path (PIP-473 P5.3).
 *
 * <p>Verifies, across a real multi-broker docker cluster, that a client discovers coordinators via
 * the {@code CommandWatchTcAssignments} stream (not the assign-topic lookup) and can drive the
 * transaction lifecycle, including after the broker leading a coordinator partition is killed.
 *
 * @see TcMetadataDiscoveryTestBase for the scope note (lifecycle, not data-in-txn).
 */
@CustomLog
public class TcMetadataDiscoveryTest extends TcMetadataDiscoveryTestBase {

    /**
     * With the scalable-topics TC enabled, a client opens the assignment watch and can open and
     * commit / abort transactions across all coordinator partitions. Running many transactions
     * exercises the round-robin spread across the watch-discovered per-leader connections.
     */
    @Test
    public void transactionLifecycleOverMetadataDiscovery() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .enableTransaction(true)
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        // Guard against a silent fallback: assert the client actually selected the metadata-store
        // assignment-watch path. Otherwise a regression that breaks the watch entirely would still
        // pass, since the assign topic is initialized with the same partition count.
        org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl tcClient =
                ((org.apache.pulsar.client.impl.PulsarClientImpl) client).getTcClient();
        assertTrue(tcClient.isUsingMetadataDiscovery(),
                "client should use metadata-store TC discovery, not the assign-topic fallback");

        // Run transactions (commit and abort alternately) until every coordinator partition has
        // minted at least one — proving the client discovered and connected to each partition's
        // elected leader. An await loop tolerates the brief startup window where a partition is
        // still mid-election and absent from the assignment snapshot.
        Set<Long> coordinatorsExercised = new HashSet<>();
        final int[] i = {0};
        Awaitility.await()
                .atMost(1, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    Transaction txn = client.newTransaction()
                            .withTransactionTimeout(1, TimeUnit.MINUTES)
                            .build().get();
                    TxnID txnId = txn.getTxnID();
                    assertNotNull(txnId);
                    // mostSigBits is the coordinator (TC partition) that minted the txn.
                    coordinatorsExercised.add(txnId.getMostSigBits());
                    if (i[0]++ % 2 == 0) {
                        txn.commit().get();
                    } else {
                        txn.abort().get();
                    }
                    return coordinatorsExercised.size() == TC_PARALLELISM;
                });
        assertEquals(coordinatorsExercised.size(), TC_PARALLELISM,
                "expected transactions to be coordinated by every TC partition; got "
                        + coordinatorsExercised);
    }

    /**
     * Kill one broker and confirm the client keeps working: the coordinator partitions that broker
     * was leading are re-elected to the survivor, the client's assignment watch receives the new
     * snapshot, retargets its handlers, and subsequent transactions across all partitions still
     * succeed.
     */
    @Test
    public void transactionsSurviveLeaderBrokerFailure() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .enableTransaction(true)
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .operationTimeout(30, TimeUnit.SECONDS)
                .build();

        // Warm up: confirm every coordinator is reachable before the failure.
        runTxnOnEveryCoordinator(client);

        // Kill one broker — about half the coordinator partitions lose their leader.
        BrokerContainer victim = pulsarCluster.getBrokers().iterator().next();
        log.info().attr("broker", victim.getContainerName()).log("Stopping broker to force TC failover");
        victim.stop();

        // After re-election + assignment-watch refresh, transactions across all partitions succeed
        // again. runTxnOnEveryCoordinator already retries within a bounded wait while leadership and
        // the client's handlers converge on the new leaders.
        runTxnOnEveryCoordinator(client);
    }

    /**
     * Open + commit one transaction on each coordinator partition; asserts all are covered within a
     * bounded wait. A coordinator's handler connects asynchronously (and, after a failover, may be
     * briefly mid-reconnect), so a transaction routed to a not-yet-ready coordinator throws
     * {@code MetaStoreHandlerNotReadyException} / times out — those are retried rather than failing
     * the run. The assertion is "every coordinator becomes reachable", not "reachable on the first
     * attempt".
     */
    private void runTxnOnEveryCoordinator(PulsarClient client) {
        Set<Long> coordinators = new HashSet<>();
        Awaitility.await()
                .atMost(90, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> {
                    Transaction txn = client.newTransaction()
                            .withTransactionTimeout(1, TimeUnit.MINUTES)
                            .build().get();
                    coordinators.add(txn.getTxnID().getMostSigBits());
                    txn.commit().get();
                    return coordinators.size() == TC_PARALLELISM;
                });
        assertTrue(coordinators.size() == TC_PARALLELISM,
                "expected all " + TC_PARALLELISM + " coordinators reachable; got " + coordinators);
    }
}
