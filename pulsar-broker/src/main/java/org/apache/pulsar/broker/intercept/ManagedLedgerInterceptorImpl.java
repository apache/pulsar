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

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.interceptor.ManagedLedgerInterceptor;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


public class ManagedLedgerInterceptorImpl implements ManagedLedgerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerInterceptorImpl.class);
    private static final String OFFSET = "offset";

    private AtomicLong offsetGenerator;
    private Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

    public ManagedLedgerInterceptorImpl() {
        offsetGenerator = new AtomicLong(-1);
    }

    public ManagedLedgerInterceptorImpl(AtomicLong offsetGenerator,
                                        Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors) {
        this.offsetGenerator = offsetGenerator;
        this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;
    }

    @Override
    public OpAddEntry beforeAddEntry(OpAddEntry op, int batchSize) {
        assert op != null;
        assert batchSize > 0;
        op.setData(Commands.addBrokerEntryMetadata(op.getData(),
                brokerEntryMetadataInterceptors, offsetGenerator, batchSize));
        return op;
    }

    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {
        assert propertiesMap != null;
        assert propertiesMap.size() > 0;

        if (propertiesMap.containsKey(OFFSET)) {
            offsetGenerator.set(Long.parseLong(propertiesMap.get(OFFSET)));
        }
    }

    @Override
    public void onManagedLedgerLastLedgerInitialize(String name, LedgerHandle lh) {
        try {
            LedgerEntries ledgerEntries =
                    lh.read(lh.getLastAddConfirmed() - 1, lh.getLastAddConfirmed());
            for (LedgerEntry entry : ledgerEntries) {
                PulsarApi.BrokerEntryMetadata brokerEntryMetadata =
                        Commands.parseBrokerEntryMetadataIfExist(entry.getEntryBuffer());
                if (brokerEntryMetadata != null) {
                    if (brokerEntryMetadata.hasOffset() && offsetGenerator.get() < brokerEntryMetadata.getOffset()) {
                        offsetGenerator.set(brokerEntryMetadata.getOffset());
                    }
                }
            }
        } catch (org.apache.bookkeeper.client.api.BKException | InterruptedException e) {
            log.error("[{}] Read last entry error.", name, e);
        }
    }
}
