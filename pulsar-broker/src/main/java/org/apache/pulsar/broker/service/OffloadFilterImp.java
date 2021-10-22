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
package org.apache.pulsar.broker.service;

import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.OffloadFilter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;

public class OffloadFilterImp implements OffloadFilter {
    PersistentTopic persistentTopic;
    public OffloadFilterImp(PersistentTopic persistentTopic) {
        this.persistentTopic = persistentTopic;
    }

    @Override
    public boolean checkIfNeedOffload(LedgerEntry ledgerEntry) {
        MessageMetadata messageMetadata = Commands.parseMessageMetadata(ledgerEntry.getEntryBuffer());

        if (messageMetadata.hasTxnidLeastBits() && messageMetadata.hasTxnidMostBits()){
            return !persistentTopic.isTxnAborted(new TxnID(messageMetadata.getTxnidMostBits(),
                    messageMetadata.getTxnidLeastBits())) && !Markers.isTxnMarker(messageMetadata);
        }
        return true;
    }

    @Override
    public boolean checkIfLedgerIdCanOffload(long ledgerId) {
        return ledgerId <= persistentTopic.getMaxReadPosition().getLedgerId();
    }

    @Override
    public boolean checkFilterIsReady() {
        return persistentTopic.getBrokerService().getPulsar().getConfiguration().isTransactionCoordinatorEnabled()
                && !"Initializing".equals(persistentTopic.getTransactionBufferStats().state);
    }
}
