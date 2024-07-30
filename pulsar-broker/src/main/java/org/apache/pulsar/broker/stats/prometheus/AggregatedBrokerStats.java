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
package org.apache.pulsar.broker.stats.prometheus;

public class AggregatedBrokerStats {
    public int topicsCount;
    public int subscriptionsCount;
    public int producersCount;
    public int consumersCount;
    public double rateIn;
    public double rateOut;
    public double throughputIn;
    public double throughputOut;
    public long storageSize;
    public long storageLogicalSize;
    public double storageWriteRate;
    public double storageReadRate;
    public double storageReadCacheMissesRate;
    public long msgBacklog;
    public long sizeBasedBacklogQuotaExceededEvictionCount;
    public long timeBasedBacklogQuotaExceededEvictionCount;
    public long bytesInCounter;
    public long bytesOutCounter;
    public long systemTopicBytesInCounter;
    public long bytesOutInternalCounter;

    @SuppressWarnings("DuplicatedCode")
    void updateStats(TopicStats stats) {
        topicsCount++;
        subscriptionsCount += stats.subscriptionsCount;
        producersCount += stats.producersCount;
        consumersCount += stats.consumersCount;
        rateIn += stats.rateIn;
        rateOut += stats.rateOut;
        throughputIn += stats.throughputIn;
        throughputOut += stats.throughputOut;
        storageSize += stats.managedLedgerStats.storageSize;
        storageLogicalSize += stats.managedLedgerStats.storageLogicalSize;
        storageWriteRate += stats.managedLedgerStats.storageWriteRate;
        storageReadRate += stats.managedLedgerStats.storageReadRate;
        storageReadCacheMissesRate += stats.managedLedgerStats.storageReadCacheMissesRate;
        msgBacklog += stats.msgBacklog;
        timeBasedBacklogQuotaExceededEvictionCount += stats.timeBasedBacklogQuotaExceededEvictionCount;
        sizeBasedBacklogQuotaExceededEvictionCount += stats.sizeBasedBacklogQuotaExceededEvictionCount;
        bytesInCounter += stats.bytesInCounter;
        bytesOutCounter += stats.bytesOutCounter;
        systemTopicBytesInCounter += stats.systemTopicBytesInCounter;
        bytesOutInternalCounter += stats.bytesOutInternalCounter;
    }

    @SuppressWarnings("DuplicatedCode")
    public void reset() {
        topicsCount = 0;
        subscriptionsCount = 0;
        producersCount = 0;
        consumersCount = 0;
        rateIn = 0;
        rateOut = 0;
        throughputIn = 0;
        throughputOut = 0;
        storageSize = 0;
        storageLogicalSize = 0;
        storageWriteRate = 0;
        storageReadRate = 0;
        storageReadCacheMissesRate = 0;
        msgBacklog = 0;
        sizeBasedBacklogQuotaExceededEvictionCount = 0;
        timeBasedBacklogQuotaExceededEvictionCount = 0;
        bytesInCounter = 0;
        bytesOutCounter = 0;
        systemTopicBytesInCounter = 0;
        bytesOutInternalCounter = 0;
    }
}
