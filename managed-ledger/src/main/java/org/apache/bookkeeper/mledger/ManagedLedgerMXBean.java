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
package org.apache.bookkeeper.mledger;

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

/**
 * Management Bean for a {@link ManagedLedger}.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface ManagedLedgerMXBean {

    /**
     * @return the ManagedLedger name
     */
    String getName();

    /**
     * @return the total size of the messages in active ledgers (accounting for the multiple copies stored)
     */
    long getStoredMessagesSize();

    /**
     * @return the total size of the messages in active ledgers (without accounting for replicas)
     */
    long getStoredMessagesLogicalSize();

    /**
     * @return the number of backlog messages for all the consumers
     */
    long getNumberOfMessagesInBacklog();

    /**
     * @return the msg/s rate of messages added
     */
    double getAddEntryMessagesRate();

    /**
     * @return the bytes/s rate of messages added
     */
    double getAddEntryBytesRate();

    /**
     * @return the bytes/s rate of messages added with replicas
     */
    double getAddEntryWithReplicasBytesRate();

    /**
     * @return the msg/s rate of messages read
     */
    double getReadEntriesRate();

    /**
     * @return the bytes/s rate of messages read
     */
    double getReadEntriesBytesRate();

    /**
     * @return the rate of mark-delete ops/s
     */
    double getMarkDeleteRate();

    /**
     * @return the number of addEntry requests that succeeded
     */
    long getAddEntrySucceed();

    /**
     * @return the number of addEntry requests that failed
     */
    long getAddEntryErrors();

    /**
     * @return the number of readEntries requests that succeeded
     */
    long getReadEntriesSucceeded();

    /**
     * @return the number of readEntries requests that failed
     */
    long getReadEntriesErrors();

    // Entry size statistics

    double getEntrySizeAverage();

    long[] getEntrySizeBuckets();

    // Add entry latency statistics

    double getAddEntryLatencyAverageUsec();

    long[] getAddEntryLatencyBuckets();

    long[] getLedgerSwitchLatencyBuckets();

    double getLedgerSwitchLatencyAverageUsec();

    StatsBuckets getInternalAddEntryLatencyBuckets();

    StatsBuckets getInternalEntrySizeBuckets();

    PendingBookieOpsStats getPendingBookieOpsStats();

    double getLedgerAddEntryLatencyAverageUsec();

    long[] getLedgerAddEntryLatencyBuckets();

    StatsBuckets getInternalLedgerAddEntryLatencyBuckets();
}
