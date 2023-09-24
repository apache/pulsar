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
package org.apache.pulsar.broker.stats.prometheus;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.HashMap;
import org.apache.bookkeeper.mledger.util.StatsBuckets;
import org.testng.annotations.Test;

@Test(groups = {"broker"})
public class AggregatedNamespaceStatsTest {

    @Test
    public void testSimpleAggregation() {
        final String namespace = "tenant/cluster/ns";

        TopicStats topicStats1 = new TopicStats();
        topicStats1.subscriptionsCount = 2;
        topicStats1.producersCount = 1;
        topicStats1.consumersCount = 3;
        topicStats1.rateIn = 10.0;
        topicStats1.rateOut = 20.0;
        topicStats1.throughputIn = 10240.0;
        topicStats1.throughputOut = 20480.0;
        topicStats1.managedLedgerStats.storageSize = 5120;
        topicStats1.managedLedgerStats.storageLogicalSize = 2048;
        topicStats1.msgBacklog = 30;
        topicStats1.managedLedgerStats.storageWriteRate = 12.0;
        topicStats1.managedLedgerStats.storageReadRate = 6.0;
        topicStats1.compactionRemovedEventCount = 10;
        topicStats1.compactionSucceedCount = 1;
        topicStats1.compactionFailedCount = 2;
        topicStats1.compactionDurationTimeInMills = 1000;
        topicStats1.compactionReadThroughput = 15.0;
        topicStats1.compactionWriteThroughput = 20.0;
        topicStats1.compactionCompactedEntriesCount = 30;
        topicStats1.compactionCompactedEntriesSize = 1000;

        AggregatedReplicationStats replStats1 = new AggregatedReplicationStats();
        replStats1.msgRateIn = 1.0;
        replStats1.msgThroughputIn = 126.0;
        replStats1.msgRateOut = 2.0;
        replStats1.msgThroughputOut = 256.0;
        replStats1.replicationBacklog = 1;
        replStats1.connectedCount = 0;
        replStats1.msgRateExpired = 3.0;
        replStats1.replicationDelayInSeconds = 20;
        topicStats1.replicationStats.put(namespace, replStats1);

        AggregatedSubscriptionStats subStats1 = new AggregatedSubscriptionStats();
        subStats1.msgBacklog = 50;
        subStats1.msgRateRedeliver = 1.5;
        subStats1.unackedMessages = 2;
        subStats1.msgBacklogNoDelayed = 30;
        topicStats1.subscriptionStats.put(namespace, subStats1);

        TopicStats topicStats2 = new TopicStats();
        topicStats2.subscriptionsCount = 10;
        topicStats2.producersCount = 3;
        topicStats2.consumersCount = 5;
        topicStats2.rateIn = 0.1;
        topicStats2.rateOut = 0.5;
        topicStats2.throughputIn = 512.0;
        topicStats2.throughputOut = 1024.5;
        topicStats2.managedLedgerStats.storageSize = 1024;
        topicStats2.managedLedgerStats.storageLogicalSize = 512;
        topicStats2.msgBacklog = 7;
        topicStats2.managedLedgerStats.storageWriteRate = 5.0;
        topicStats2.managedLedgerStats.storageReadRate = 2.5;
        topicStats2.compactionRemovedEventCount = 10;
        topicStats2.compactionSucceedCount = 1;
        topicStats2.compactionFailedCount = 2;
        topicStats2.compactionDurationTimeInMills = 1000;
        topicStats2.compactionReadThroughput = 15.0;
        topicStats2.compactionWriteThroughput = 20.0;
        topicStats2.compactionCompactedEntriesCount = 30;
        topicStats2.compactionCompactedEntriesSize = 1000;

        AggregatedReplicationStats replStats2 = new AggregatedReplicationStats();
        replStats2.msgRateIn = 3.5;
        replStats2.msgThroughputIn = 512.0;
        replStats2.msgRateOut = 10.5;
        replStats2.msgThroughputOut = 1536.0;
        replStats2.replicationBacklog = 99;
        replStats2.connectedCount = 1;
        replStats2.msgRateExpired = 3.0;
        replStats2.replicationDelayInSeconds = 20;
        topicStats2.replicationStats.put(namespace, replStats2);

        AggregatedSubscriptionStats subStats2 = new AggregatedSubscriptionStats();
        subStats2.msgBacklog = 27;
        subStats2.msgRateRedeliver = 0.7;
        subStats2.unackedMessages = 0;
        subStats2.msgBacklogNoDelayed = 20;
        topicStats2.subscriptionStats.put(namespace, subStats2);

        AggregatedNamespaceStats nsStats = new AggregatedNamespaceStats();
        nsStats.updateStats(topicStats1);
        nsStats.updateStats(topicStats2);

        assertEquals(nsStats.topicsCount, 2);
        assertEquals(nsStats.subscriptionsCount, 12);
        assertEquals(nsStats.producersCount, 4);
        assertEquals(nsStats.consumersCount, 8);
        assertEquals(nsStats.rateIn, 10.1);
        assertEquals(nsStats.rateOut, 20.5);
        assertEquals(nsStats.throughputIn, 10752.0);
        assertEquals(nsStats.throughputOut, 21504.5);
        assertEquals(nsStats.managedLedgerStats.storageSize, 6144);
        assertEquals(nsStats.msgBacklog, 37);
        assertEquals(nsStats.managedLedgerStats.storageWriteRate, 17.0);
        assertEquals(nsStats.managedLedgerStats.storageReadRate, 8.5);
        assertEquals(nsStats.managedLedgerStats.storageSize, 6144);
        assertEquals(nsStats.managedLedgerStats.storageLogicalSize, 2560);

        assertEquals(nsStats.compactionRemovedEventCount, 20);
        assertEquals(nsStats.compactionSucceedCount, 2);
        assertEquals(nsStats.compactionFailedCount, 4);
        assertEquals(nsStats.compactionDurationTimeInMills, 2000);
        assertEquals(nsStats.compactionReadThroughput, 30.0);
        assertEquals(nsStats.compactionWriteThroughput, 40.0);
        assertEquals(nsStats.compactionCompactedEntriesCount, 60);
        assertEquals(nsStats.compactionCompactedEntriesSize, 2000);

        AggregatedReplicationStats nsReplStats = nsStats.replicationStats.get(namespace);
        assertNotNull(nsReplStats);
        assertEquals(nsReplStats.msgRateIn, 4.5);
        assertEquals(nsReplStats.msgThroughputIn, 638.0);
        assertEquals(nsReplStats.msgRateOut, 12.5);
        assertEquals(nsReplStats.msgThroughputOut, 1792.0);
        assertEquals(nsReplStats.replicationBacklog, 100);
        assertEquals(nsReplStats.connectedCount, 1);
        assertEquals(nsReplStats.msgRateExpired, 6.0);
        assertEquals(nsReplStats.replicationDelayInSeconds, 40);

        AggregatedSubscriptionStats nsSubStats = nsStats.subscriptionStats.get(namespace);
        assertNotNull(nsSubStats);
        assertEquals(nsSubStats.msgBacklog, 77);
        assertEquals(nsSubStats.msgBacklogNoDelayed, 50);
        assertEquals(nsSubStats.msgRateRedeliver, 2.2);
        assertEquals(nsSubStats.unackedMessages, 2);
    }


    @Test
    public void testReset() {
        AggregatedNamespaceStats stats = new AggregatedNamespaceStats();
        stats.topicsCount = 8;
        stats.subscriptionsCount = 3;
        stats.producersCount = 1;
        stats.consumersCount = 8;
        stats.rateIn = 1.3;
        stats.rateOut = 3.5;
        stats.throughputIn = 3.2;
        stats.throughputOut = 5.8;
        stats.messageAckRate = 12;
        stats.bytesInCounter = 1234;
        stats.msgInCounter = 3889;
        stats.bytesOutCounter = 89775;
        stats.msgOutCounter = 28983;
        stats.msgBacklog = 39;
        stats.msgDelayed = 31;

        stats.backlogQuotaLimit = 387;
        stats.backlogQuotaLimitTime = 8771;

        stats.replicationStats = new HashMap<>();
        stats.replicationStats.put("r", new AggregatedReplicationStats());

        stats.subscriptionStats = new HashMap<>();
        stats.subscriptionStats.put("r", new AggregatedSubscriptionStats());

        stats.compactionRemovedEventCount = 124;
        stats.compactionSucceedCount = 487;
        stats.compactionFailedCount = 84857;
        stats.compactionDurationTimeInMills = 2384;
        stats.compactionReadThroughput = 355423;
        stats.compactionWriteThroughput = 23299;
        stats.compactionCompactedEntriesCount = 37522;
        stats.compactionCompactedEntriesSize = 8475;

        stats.compactionLatencyBuckets = new StatsBuckets(5);
        stats.compactionLatencyBuckets.addValue(3);

        stats.reset();

        assertEquals(stats.bytesOutCounter, 0);
        assertEquals(stats.topicsCount, 0);
        assertEquals(stats.subscriptionsCount, 0);
        assertEquals(stats.producersCount, 0);
        assertEquals(stats.consumersCount, 0);
        assertEquals(stats.rateIn, 0);
        assertEquals(stats.rateOut, 0);
        assertEquals(stats.throughputIn, 0);
        assertEquals(stats.throughputOut, 0);
        assertEquals(stats.messageAckRate, 0);
        assertEquals(stats.bytesInCounter, 0);
        assertEquals(stats.msgInCounter, 0);
        assertEquals(stats.bytesOutCounter, 0);
        assertEquals(stats.msgOutCounter, 0);

        assertEquals(stats.managedLedgerStats.storageSize, 0);

        assertEquals(stats.msgBacklog, 0);
        assertEquals(stats.msgDelayed, 0);

        assertEquals(stats.backlogQuotaLimit, 0);
        assertEquals(stats.backlogQuotaLimitTime, -1);

        assertEquals(stats.replicationStats.size(), 0);
        assertEquals(stats.subscriptionStats.size(), 0);

        assertEquals(stats.compactionRemovedEventCount, 0);
        assertEquals(stats.compactionSucceedCount, 0);
        assertEquals(stats.compactionFailedCount, 0);
        assertEquals(stats.compactionDurationTimeInMills, 0);
        assertEquals(stats.compactionReadThroughput, 0);
        assertEquals(stats.compactionWriteThroughput, 0);
        assertEquals(stats.compactionCompactedEntriesCount, 0);
        assertEquals(stats.compactionCompactedEntriesSize, 0);

        assertEquals(stats.compactionLatencyBuckets.getSum(), 0);
    }

}
