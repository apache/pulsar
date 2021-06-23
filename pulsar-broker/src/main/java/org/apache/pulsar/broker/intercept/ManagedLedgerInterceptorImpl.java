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
package org.apache.pulsar.broker.intercept;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerInterceptorImpl implements ManagedLedgerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerInterceptorImpl.class);
    private static final String INDEX = "index";


    private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;


    public ManagedLedgerInterceptorImpl(Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors) {
        this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;
    }

    public long getIndex() {
        long index = -1;
        for (BrokerEntryMetadataInterceptor interceptor : brokerEntryMetadataInterceptors) {
            if (interceptor instanceof AppendIndexMetadataInterceptor) {
                index = ((AppendIndexMetadataInterceptor) interceptor).getIndex();
            }
        }
        return index;
    }

    @Override
    public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
       if (op == null || numberOfMessages <= 0) {
           return op;
       }
        op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors, numberOfMessages));
        return op;
    }

    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {
        if (propertiesMap == null || propertiesMap.size() == 0) {
            return;
        }

        if (propertiesMap.containsKey(INDEX)) {
            for (BrokerEntryMetadataInterceptor interceptor : brokerEntryMetadataInterceptors) {
                if (interceptor instanceof AppendIndexMetadataInterceptor) {
                  ((AppendIndexMetadataInterceptor) interceptor)
                          .recoveryIndexGenerator(Long.parseLong(propertiesMap.get(INDEX)));
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> onManagedLedgerLastLedgerInitialize(String name, LedgerHandle lh) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        boolean hasAppendIndexMetadataInterceptor = brokerEntryMetadataInterceptors.stream()
                .anyMatch(interceptor -> interceptor instanceof AppendIndexMetadataInterceptor);
        if (hasAppendIndexMetadataInterceptor && lh.getLastAddConfirmed() >= 0) {
            lh.readAsync(lh.getLastAddConfirmed(), lh.getLastAddConfirmed()).whenComplete((entries, ex) -> {
                if (ex != null) {
                    log.error("[{}] Read last entry error.", name, ex);
                    promise.completeExceptionally(ex);
                } else {
                    if (entries != null) {
                        try {
                            LedgerEntry ledgerEntry = entries.getEntry(lh.getLastAddConfirmed());
                            if (ledgerEntry != null) {
                                BrokerEntryMetadata brokerEntryMetadata =
                                        Commands.parseBrokerEntryMetadataIfExist(ledgerEntry.getEntryBuffer());
                                for (BrokerEntryMetadataInterceptor interceptor : brokerEntryMetadataInterceptors) {
                                    if (interceptor instanceof AppendIndexMetadataInterceptor) {
                                        if (brokerEntryMetadata != null && brokerEntryMetadata.hasIndex()) {
                                            ((AppendIndexMetadataInterceptor) interceptor)
                                                    .recoveryIndexGenerator(brokerEntryMetadata.getIndex());
                                        }
                                    }
                                }
                            }
                            entries.close();
                            promise.complete(null);
                        } catch (Exception e) {
                            log.error("[{}] Failed to recover the index generator from the last add confirmed entry.",
                                    name, e);
                            promise.completeExceptionally(e);
                        }
                    } else {
                        promise.complete(null);
                    }
                }
            });
        } else {
            promise.complete(null);
        }
        return promise;
    }

    @Override
    public void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap) {
        for (BrokerEntryMetadataInterceptor interceptor : brokerEntryMetadataInterceptors) {
            if (interceptor instanceof AppendIndexMetadataInterceptor) {
                propertiesMap.put(INDEX, String.valueOf(((AppendIndexMetadataInterceptor) interceptor).getIndex()));
            }
        }
    }
}
