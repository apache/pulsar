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

import java.util.Set;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.apache.pulsar.common.protocol.Commands;

public class MockManagedLedgerInterceptorImpl extends ManagedLedgerInterceptorImpl {
    private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

    public MockManagedLedgerInterceptorImpl(
            Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors,
            Set<ManagedLedgerPayloadProcessor> brokerEntryPayloadProcessors) {
        super(brokerEntryMetadataInterceptors, brokerEntryPayloadProcessors);
        this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;
    }

    @Override
    public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
        if (op == null || numberOfMessages <= 0) {
            return op;
        }
        op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors,
                numberOfMessages));
        if (op != null) {
            throw new RuntimeException("throw exception before add entry for test");
        }
        return op;
    }
}
