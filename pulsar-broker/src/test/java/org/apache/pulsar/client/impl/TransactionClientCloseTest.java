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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.CoordinatorClientStateException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class TransactionClientCloseTest extends SharedPulsarBaseTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    public void testEndTransactionAfterClientClose() throws Exception {
        @Cleanup
        PulsarClient client = newTransactionClient();
        Transaction txnToCommit = client.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();
        Transaction txnToAbort = client.newTransaction().withTransactionTimeout(1, TimeUnit.MINUTES).build().get();

        client.close();

        // Ending a transaction after its client closed must fail instead of never completing.
        assertFailsWithStateException("commit", txnToCommit.commit());
        assertFailsWithStateException("abort", txnToAbort.abort());
    }

    @Test
    public void testCoordinatorRequestsFailAfterClientClose() throws Exception {
        @Cleanup
        PulsarClient client = newTransactionClient();
        TransactionCoordinatorClient tcClient = client.getTransactionCoordinatorClient();

        client.close();

        assertFailsWithStateException("newTransaction", client.newTransaction().build());
        assertRequestsFail(tcClient);
        assertThat(tcClient.getState()).as("coordinator client state")
                .isEqualTo(TransactionCoordinatorClient.State.CLOSED);
    }

    @Test
    public void testCoordinatorRequestsFailBeforeStart() {
        assertRequestsFail(new TransactionCoordinatorClientImpl(pulsarClient));
    }

    private PulsarClient newTransactionClient() throws Exception {
        return PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .enableTransaction(true)
                .build();
    }

    private static void assertRequestsFail(TransactionCoordinatorClient tcClient) {
        TxnID txnId = new TxnID(0, 0);
        assertFailsWithStateException("newTransactionAsync", tcClient.newTransactionAsync());
        assertFailsWithStateException("addPublishPartitionToTxnAsync",
                tcClient.addPublishPartitionToTxnAsync(txnId, List.of("topic")));
        assertFailsWithStateException("addSubscriptionToTxnAsync",
                tcClient.addSubscriptionToTxnAsync(txnId, "topic", "subscription"));
        assertFailsWithStateException("commitAsync", tcClient.commitAsync(txnId));
        assertFailsWithStateException("abortAsync", tcClient.abortAsync(txnId));
    }

    private static void assertFailsWithStateException(String operation, CompletableFuture<?> future) {
        assertThat(future).as(operation)
                .failsWithin(TIMEOUT)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(CoordinatorClientStateException.class);
    }
}
