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
package org.apache.pulsar.testclient.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.testclient.PerformanceProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceUtils {

    private static final Logger log = LoggerFactory.getLogger(PerformanceProducer.class);

    public static AtomicReference<Transaction> buildTransaction(PulsarClient pulsarClient, boolean isEnableTransaction,
                                                                long transactionTimeout) {

        AtomicLong numBuildTxnFailed = new AtomicLong();
        if (isEnableTransaction) {
            while(true) {
                AtomicReference atomicReference = null;
                try {
                    atomicReference = new AtomicReference(pulsarClient.newTransaction()
                            .withTransactionTimeout(transactionTimeout, TimeUnit.SECONDS).build().get());
                } catch (Exception e) {
                    numBuildTxnFailed.incrementAndGet();
                    if (numBuildTxnFailed.get()%10 == 0) {
                        log.error("Failed to new a transaction with {} times", numBuildTxnFailed.get(), e);
                    }
                }
                if (atomicReference != null && atomicReference.get() != null) {
                    log.info("After {} failures, the transaction was created successfully for the first time",
                            numBuildTxnFailed.get());
                    return atomicReference;
                }
            }
        }
        return new AtomicReference<>(null);
    }
}
