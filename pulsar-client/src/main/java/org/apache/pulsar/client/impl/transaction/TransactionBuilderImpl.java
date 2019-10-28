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
package org.apache.pulsar.client.impl.transaction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.apache.pulsar.client.impl.PulsarClientImpl;

/**
 * The default implementation of transaction builder to build transactions.
 */
public class TransactionBuilderImpl implements TransactionBuilder {

    private final PulsarClientImpl client;
    private long txnTimeoutMs = 60000; // 1 minute

    public TransactionBuilderImpl(PulsarClientImpl client) {
        this.client = client;
    }

    @Override
    public TransactionBuilder withTransactionTimeout(long timeout, TimeUnit timeoutUnit) {
        this.txnTimeoutMs = timeoutUnit.toMillis(timeout);
        return this;
    }

    @Override
    public CompletableFuture<Transaction> build() {
        // TODO: talk to TC to begin a transaction
        //       the builder is responsible for locating the transaction coorindator (TC)
        //       and start the transaction to get the transaction id.
        //       After getting the transaction id, all the operations are handled by the
        //       `TransactionImpl`
        return CompletableFuture.completedFuture(
            new TransactionImpl(client, txnTimeoutMs, -1L, -1L));
    }
}
