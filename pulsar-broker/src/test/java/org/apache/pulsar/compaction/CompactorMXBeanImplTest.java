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
package org.apache.pulsar.compaction;

import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Test(groups = "broker-compaction")
public class CompactorMXBeanImplTest {

    @Test
    public void testSimple() throws Exception {
        CompactorMXBeanImpl mxBean = new CompactorMXBeanImpl();
        String topic = "topic1";
        mxBean.addCompactionStartOp(topic);
        CompactionRecord compaction = mxBean.getCompactionRecordForTopic(topic).get();
        assertEquals(compaction.getLastCompactionRemovedEventCount(), 0, 0);
        mxBean.addCompactionRemovedEvent(topic);
        assertEquals(compaction.getLastCompactionRemovedEventCount(), 0, 0);
        mxBean.addCompactionEndOp(topic, true);
        assertEquals(compaction.getLastCompactionRemovedEventCount(), 1, 0);
        assertTrue(compaction.getLastCompactionSucceedTimestamp() > 0L);
        assertTrue(compaction.getLastCompactionDurationTimeInMills() >= 0L);
        assertEquals(compaction.getLastCompactionFailedTimestamp(), 0, 0);
        assertEquals(compaction.getCompactionRemovedEventCount(), 1, 0);
        assertEquals(compaction.getCompactionSucceedCount(), 1, 0);
        assertEquals(compaction.getCompactionFailedCount(), 0, 0);
        assertTrue(compaction.getCompactionDurationTimeInMills() >= 0L);
        mxBean.addCompactionStartOp(topic);
        mxBean.addCompactionRemovedEvent(topic);
        mxBean.addCompactionEndOp(topic, false);
        assertEquals(compaction.getCompactionRemovedEventCount(), 2, 0);
        assertEquals(compaction.getCompactionFailedCount(), 1, 0);
        assertEquals(compaction.getCompactionSucceedCount(), 1, 0);
        assertTrue(compaction.getLastCompactionFailedTimestamp() > 0L);
        assertTrue(compaction.getCompactionDurationTimeInMills() >= 0L);
        mxBean.addCompactionReadOp(topic, 22);
        assertTrue(compaction.getCompactionReadThroughput() > 0L);
        mxBean.addCompactionWriteOp(topic, 33);
        assertTrue(compaction.getCompactionWriteThroughput() > 0L);
        mxBean.addCompactionLatencyOp(topic, 10, TimeUnit.NANOSECONDS);
        assertTrue(compaction.getCompactionLatencyBuckets()[0] > 0l);
        mxBean.reset();
        assertEquals(compaction.getCompactionRemovedEventCount(), 0, 0);
        assertEquals(compaction.getCompactionSucceedCount(), 0, 0);
        assertEquals(compaction.getCompactionFailedCount(), 0, 0);
        assertEquals(compaction.getCompactionDurationTimeInMills(), 0, 0);
    }

}
