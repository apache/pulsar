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
package org.apache.pulsar.client.api.transaction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * The builder to build a transaction for Pulsar.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TransactionBuilder {

    /**
     * Configure the maximum amount of time that the transaction
     * coordinator will for a transaction to be completed by the
     * client before proactively aborting the ongoing transaction.
     *
     * <p>The config value will be sent to the transaction coordinator
     * along with the CommandNewTxn. Default is 60 seconds.
     *
     * @param timeout the transaction timeout value
     * @param timeoutUnit the transaction timeout unit
     * @return the transaction builder itself
     */
    TransactionBuilder withTransactionTimeout(long timeout, TimeUnit timeoutUnit);

    /**
     * Build the transaction with the configured settings.
     *
     * @return a future represents the result of starting a new transaction
     */
    CompletableFuture<Transaction> build();

}
