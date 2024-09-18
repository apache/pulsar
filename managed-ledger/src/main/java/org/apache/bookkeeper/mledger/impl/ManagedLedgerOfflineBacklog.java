/*
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
package org.apache.bookkeeper.mledger.impl;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ManagedLedgerOfflineBacklog {

    private final byte[] password;
    private final BookKeeper.DigestType digestType;
    private static final int META_READ_TIMEOUT_SECONDS = 60;
    private final boolean accurate;
    private final String brokerName;

    public ManagedLedgerOfflineBacklog(DigestType digestType, byte[] password, String brokerName,
            boolean accurate) {
        this.digestType = BookKeeper.DigestType.fromApiDigestType(digestType);
        this.password = password;
        this.accurate = accurate;
        this.brokerName = brokerName;
    }

    // need a better way than to duplicate the functionality below from ML
    private long getNumberOfEntries(Range<Position> range,
            NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers) {
        Position fromPosition = range.lowerEndpoint();
        boolean fromIncluded = range.lowerBoundType() == BoundType.CLOSED;
        Position toPosition = range.upperEndpoint();
        boolean toIncluded = range.upperBoundType() == BoundType.CLOSED;

        if (fromPosition.getLedgerId() == toPosition.getLedgerId()) {
            // If the 2 positions are in the same ledger
            long count = toPosition.getEntryId() - fromPosition.getEntryId() - 1;
            count += fromIncluded ? 1 : 0;
            count += toIncluded ? 1 : 0;
            return count;
        } else {
            long count = 0;
            // If the from & to are pointing to different ledgers, then we need to :
            // 1. Add the entries in the ledger pointed by toPosition
            count += toPosition.getEntryId();
            count += toIncluded ? 1 : 0;

            // 2. Add the entries in the ledger pointed by fromPosition
            MLDataFormats.ManagedLedgerInfo.LedgerInfo li = ledgers.get(fromPosition.getLedgerId());
            if (li != null) {
                count += li.getEntries() - (fromPosition.getEntryId() + 1);
                count += fromIncluded ? 1 : 0;
            }

            // 3. Add the whole ledgers entries in between
            for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ls : ledgers
                    .subMap(fromPosition.getLedgerId(), false, toPosition.getLedgerId(), false).values()) {
                count += ls.getEntries();
            }

            return count;
        }
    }

    public PersistentOfflineTopicStats getEstimatedUnloadedTopicBacklog(ManagedLedgerFactory factory,
            String managedLedgerName) throws Exception {
        return estimateUnloadedTopicBacklog(factory, TopicName.get("persistent://" + managedLedgerName));
    }

    public PersistentOfflineTopicStats estimateUnloadedTopicBacklog(ManagedLedgerFactory factory,
                                                                    TopicName topicName) throws Exception {
        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        final PersistentOfflineTopicStats offlineTopicStats = new PersistentOfflineTopicStats(managedLedgerName,
                brokerName);
        if (factory instanceof ManagedLedgerFactoryImpl) {
            List<Object> ctx = new ArrayList<>();
            ctx.add(digestType);
            ctx.add(password);
            factory.estimateUnloadedTopicBacklog(offlineTopicStats, topicName, accurate, ctx);
        } else {
            Object ctx = null;
            factory.estimateUnloadedTopicBacklog(offlineTopicStats, topicName, accurate, ctx);
        }


        return offlineTopicStats;
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerOfflineBacklog.class);
}
