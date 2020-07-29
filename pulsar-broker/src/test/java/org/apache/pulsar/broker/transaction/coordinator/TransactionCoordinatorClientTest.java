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
package org.apache.pulsar.broker.transaction.coordinator;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

import com.google.common.collect.Lists;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferClientImpl;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient.State;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TransactionCoordinatorClientTest extends TransactionMetaStoreTestBase {

    @BeforeClass
    public void init() throws Exception {
        super.setup();

        for (PulsarService pulsarService : pulsarServices) {
            TransactionBufferClient transactionBufferClient = Mockito.mock(TransactionBufferClientImpl.class);
            Mockito.when(transactionBufferClient.commitTxnOnTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            Mockito.when(transactionBufferClient.abortTxnOnTopic(anyString(), anyLong(), anyLong()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            Mockito.when(transactionBufferClient.commitTxnOnSubscription(anyString(), anyString(), anyLong(), anyLong()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            Mockito.when(transactionBufferClient.abortTxnOnSubscription(anyString(), anyString(), anyLong(), anyLong()))
                    .thenReturn(CompletableFuture.completedFuture(null));

            Mockito.when(pulsarService.getTransactionBufferClient()).thenReturn(transactionBufferClient);
        }
    }

    @Test
    public void testClientStart() throws PulsarClientException, TransactionCoordinatorClientException, InterruptedException {
        try {
            transactionCoordinatorClient.start();
            Assert.fail("should failed here because the transaction metas store already started!");
        } catch (TransactionCoordinatorClientException e) {
            // ok here
        }

        Assert.assertNotNull(transactionCoordinatorClient);
        Assert.assertEquals(transactionCoordinatorClient.getState(), State.READY);
    }

    @Test
    public void testNewTxn() throws TransactionCoordinatorClientException {
        TxnID txnID = transactionCoordinatorClient.newTransaction();
        Assert.assertNotNull(txnID);
        Assert.assertEquals(txnID.getLeastSigBits(), 0L);
    }

    @Test
    public void testCommitAndAbort() throws TransactionCoordinatorClientException {
        TxnID txnID = transactionCoordinatorClient.newTransaction();
        transactionCoordinatorClient.addPublishPartitionToTxn(txnID, Lists.newArrayList("persistent://public/default/testCommitAndAbort"));
        transactionCoordinatorClient.commit(txnID);
        try {
            transactionCoordinatorClient.abort(txnID);
            Assert.fail("Should be fail, because the txn is in committing state, can't abort now.");
        } catch (TransactionCoordinatorClientException.InvalidTxnStatusException ignore) {
           // Ok here
        }
    }
}
