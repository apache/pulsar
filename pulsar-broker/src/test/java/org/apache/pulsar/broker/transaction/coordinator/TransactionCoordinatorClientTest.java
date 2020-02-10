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

import com.google.common.collect.Lists;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient.State;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TransactionCoordinatorClientTest extends TransactionMetaStoreTestBase {

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
