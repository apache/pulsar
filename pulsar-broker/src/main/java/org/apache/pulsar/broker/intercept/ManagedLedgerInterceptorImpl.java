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
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.interceptor.ManagedLedgerInterceptor;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.intercept.AppendOffsetMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerInterceptorImpl implements ManagedLedgerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerInterceptorImpl.class);
    private static final String OFFSET = "offset";


    private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;


    public ManagedLedgerInterceptorImpl(Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors) {
        this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;
    }

    public long getOffset() {
        long offset = -1;
        for (BrokerEntryMetadataInterceptor interceptor : brokerEntryMetadataInterceptors) {
            if (interceptor instanceof AppendOffsetMetadataInterceptor) {
                offset = ((AppendOffsetMetadataInterceptor) interceptor).getOffset();
            }
        }
        return offset;
    }

    @Override
    public OpAddEntry beforeAddEntry(OpAddEntry op, int batchSize) {
       if (op == null || batchSize <= 0) {
           return op;
       }
        op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors, batchSize));
        return op;
    }

    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {
        if (propertiesMap == null || propertiesMap.size() == 0) {
            return;
        }

        if (propertiesMap.containsKey(OFFSET)) {
            for (BrokerEntryMetadataInterceptor interceptor : brokerEntryMetadataInterceptors) {
                if (interceptor instanceof AppendOffsetMetadataInterceptor) {
                  ((AppendOffsetMetadataInterceptor) interceptor)
                          .recoveryOffsetGenerator(Long.parseLong(propertiesMap.get(OFFSET)));
                }
            }
        }
    }

    @Override
    public void onManagedLedgerLastLedgerInitialize(String name, LedgerHandle lh) {
        try {
            for (BrokerEntryMetadataInterceptor interceptor : brokerEntryMetadataInterceptors) {
                if (interceptor instanceof AppendOffsetMetadataInterceptor) {
                    LedgerEntries ledgerEntries =
                            lh.read(lh.getLastAddConfirmed() - 1, lh.getLastAddConfirmed());
                    for (LedgerEntry entry : ledgerEntries) {
                        PulsarApi.BrokerEntryMetadata brokerEntryMetadata =
                                Commands.parseBrokerEntryMetadataIfExist(entry.getEntryBuffer());
                        if (brokerEntryMetadata != null && brokerEntryMetadata.hasOffset()) {
                            ((AppendOffsetMetadataInterceptor) interceptor)
                                    .recoveryOffsetGenerator(brokerEntryMetadata.getOffset());
                        }
                    }

                }
            }
        } catch (org.apache.bookkeeper.client.api.BKException | InterruptedException e) {
            log.error("[{}] Read last entry error.", name, e);
        }
    }

    @Override
    public void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap) {
        for (BrokerEntryMetadataInterceptor interceptor : brokerEntryMetadataInterceptors) {
            if (interceptor instanceof AppendOffsetMetadataInterceptor) {
                propertiesMap.put(OFFSET, String.valueOf(((AppendOffsetMetadataInterceptor) interceptor).getOffset()));
            }
        }
    }
}
