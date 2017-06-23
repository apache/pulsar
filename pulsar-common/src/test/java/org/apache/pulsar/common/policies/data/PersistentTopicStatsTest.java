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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.common.policies.data.PersistentSubscriptionStats;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.testng.annotations.Test;

public class PersistentTopicStatsTest {

    @Test
    public void testPersistentTopicStats() {
        PersistentTopicStats persistentTopicStats = new PersistentTopicStats();
        persistentTopicStats.msgRateIn = 1;
        persistentTopicStats.msgThroughputIn = 1;
        persistentTopicStats.msgRateOut = 1;
        persistentTopicStats.msgThroughputOut = 1;
        persistentTopicStats.averageMsgSize = 1;
        persistentTopicStats.storageSize = 1;
        persistentTopicStats.publishers.add(new PublisherStats());
        persistentTopicStats.subscriptions.put("test_ns", new PersistentSubscriptionStats());
        persistentTopicStats.replication.put("test_ns", new ReplicatorStats());
        PersistentTopicStats target = new PersistentTopicStats();
        target.add(persistentTopicStats);
        assertEquals(persistentTopicStats.msgRateIn, 1.0);
        assertEquals(persistentTopicStats.msgThroughputIn, 1.0);
        assertEquals(persistentTopicStats.msgRateOut, 1.0);
        assertEquals(persistentTopicStats.msgThroughputOut, 1.0);
        assertEquals(persistentTopicStats.averageMsgSize, 1.0);
        assertEquals(persistentTopicStats.storageSize, 1);
        assertEquals(persistentTopicStats.publishers.size(), 1);
        assertEquals(persistentTopicStats.subscriptions.size(), 1);
        assertEquals(persistentTopicStats.replication.size(), 1);
        persistentTopicStats.reset();
        assertEquals(persistentTopicStats.msgRateIn, 0.0);
        assertEquals(persistentTopicStats.msgThroughputIn, 0.0);
        assertEquals(persistentTopicStats.msgRateOut, 0.0);
        assertEquals(persistentTopicStats.msgThroughputOut, 0.0);
        assertEquals(persistentTopicStats.averageMsgSize, 0.0);
        assertEquals(persistentTopicStats.storageSize, 0);
        assertEquals(persistentTopicStats.publishers.size(), 0);
        assertEquals(persistentTopicStats.subscriptions.size(), 0);
        assertEquals(persistentTopicStats.replication.size(), 0);
    }

}