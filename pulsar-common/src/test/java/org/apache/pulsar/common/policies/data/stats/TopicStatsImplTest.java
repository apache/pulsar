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
package org.apache.pulsar.common.policies.data.stats;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

public class TopicStatsImplTest {

    @Test
    public void testReset() {
        TopicStatsImpl stats = new TopicStatsImpl();
        stats.earliestMsgPublishTimeInBacklogs = 1L;
        stats.reset();
        assertEquals(stats.earliestMsgPublishTimeInBacklogs, 0L);
    }

    @Test
    public void testAdd_EarliestMsgPublishTimeInBacklogs_Earliest() {
        TopicStatsImpl stats1 = new TopicStatsImpl();
        stats1.earliestMsgPublishTimeInBacklogs = 10L;

        TopicStatsImpl stats2 = new TopicStatsImpl();
        stats2.earliestMsgPublishTimeInBacklogs = 20L;

        TopicStatsImpl aggregate = stats1.add(stats2);
        assertEquals(aggregate.earliestMsgPublishTimeInBacklogs, 10L);
    }

    @Test
    public void testAdd_EarliestMsgPublishTimeInBacklogs_First0() {
        TopicStatsImpl stats1 = new TopicStatsImpl();
        stats1.earliestMsgPublishTimeInBacklogs = 0L;

        TopicStatsImpl stats2 = new TopicStatsImpl();
        stats2.earliestMsgPublishTimeInBacklogs = 20L;

        TopicStatsImpl aggregate = stats1.add(stats2);
        assertEquals(aggregate.earliestMsgPublishTimeInBacklogs, 20L);
    }

    @Test
    public void testAdd_EarliestMsgPublishTimeInBacklogs_Second0() {
        TopicStatsImpl stats1 = new TopicStatsImpl();
        stats1.earliestMsgPublishTimeInBacklogs = 10L;

        TopicStatsImpl stats2 = new TopicStatsImpl();
        stats2.earliestMsgPublishTimeInBacklogs = 0L;

        TopicStatsImpl aggregate = stats1.add(stats2);
        assertEquals(aggregate.earliestMsgPublishTimeInBacklogs, 10L);
    }

    @Test
    public void testAdd_EarliestMsgPublishTimeInBacklogs_Zero() {
        TopicStatsImpl stats1 = new TopicStatsImpl();
        stats1.earliestMsgPublishTimeInBacklogs = 0L;

        TopicStatsImpl stats2 = new TopicStatsImpl();
        stats2.earliestMsgPublishTimeInBacklogs = 0L;

        TopicStatsImpl aggregate = stats1.add(stats2);
        assertEquals(aggregate.earliestMsgPublishTimeInBacklogs, 0L);
    }
}