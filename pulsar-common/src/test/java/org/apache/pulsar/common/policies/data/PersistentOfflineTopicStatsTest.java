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

import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PersistentOfflineTopicStatsTest {

    @Test
    public void testPersistentOfflineTopicStats() {
        PersistentOfflineTopicStats pot = new PersistentOfflineTopicStats("topic1",
                "prod1-broker1.messaging.use.example.com");
        String cursor = "cursor0";
        long time = System.currentTimeMillis();
        pot.addCursorDetails(cursor, 0, 1);
        pot.addLedgerDetails(0, time, 100, 1);
        Assert.assertEquals(pot.cursorDetails.get(cursor).cursorBacklog, 0);
        Assert.assertEquals(pot.cursorDetails.get(cursor).cursorLedgerId, 1);
        Assert.assertEquals(pot.dataLedgerDetails.get(0).entries, 0);
        Assert.assertEquals(pot.dataLedgerDetails.get(0).timestamp, time);
        Assert.assertEquals(pot.dataLedgerDetails.get(0).size, 100);
        Assert.assertEquals(pot.dataLedgerDetails.get(0).ledgerId, 1);
        long resetAt = System.currentTimeMillis();
        pot.reset();
        Assert.assertEquals(pot.storageSize, 0);
        Assert.assertEquals(pot.totalMessages, 0);
        Assert.assertEquals(pot.messageBacklog, 0);
        Assert.assertEquals(pot.dataLedgerDetails.size(), 0);
        Assert.assertEquals(pot.cursorDetails.size(), 0);
        Assert.assertTrue(pot.statGeneratedAt.getTime() - resetAt < 100);
    }
}
