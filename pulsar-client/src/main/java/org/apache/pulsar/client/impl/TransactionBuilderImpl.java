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
 *
 */
package org.apache.pulsar.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Transaction;
import org.apache.pulsar.client.api.TransactionBuilder;
import org.apache.pulsar.client.impl.conf.TransactionConfigurationData;

public class TransactionBuilderImpl implements TransactionBuilder {

    private final PulsarClientImpl client;
    private TransactionConfigurationData conf;

    public TransactionBuilderImpl(PulsarClientImpl client, TransactionConfigurationData conf) {
        this.client = client;
        this.conf = conf;
    }

    @Override
    public Transaction build() throws PulsarClientException {
        try {
            return buildAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<Transaction> buildAsync() {
        return client.createTransactionAsync(conf);
    }

    @Override
    public TransactionBuilder withTransactionTimeout(int timeout, TimeUnit timeoutUnit) {
        conf.setTransactionTimeout(timeout, timeoutUnit);
        return this;
    }

    @Override
    public TransactionBuilder topic(String topc) {
        conf.setTopic(topc);
        return this;
    }

    @Override
    public TransactionBuilder clone() {
        return new TransactionBuilderImpl(client, conf);
    }
}
