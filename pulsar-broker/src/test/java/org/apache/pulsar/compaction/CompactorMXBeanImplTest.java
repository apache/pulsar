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

import org.apache.pulsar.broker.service.BrokerService;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Test(groups = "broker-compaction")
public class CompactorMXBeanImplTest {

    @Test
    public void testSimple() throws Exception {
        CompactorMXBeanImpl mxBean = new CompactorMXBeanImpl();
        String topic = "topic1";
        mxBean.addCompactionStartOp(topic);
        assertEquals(mxBean.getLastCompactionRemovedEventCount(topic), 0, 0);
        mxBean.addCompactionRemovedEvent(topic);
        assertEquals(mxBean.getLastCompactionRemovedEventCount(topic), 0, 0);
        mxBean.addCompactionEndOp(topic, true);
        mxBean.addCompactionEndOp(topic, false);
        assertEquals(mxBean.getLastCompactionRemovedEventCount(topic), 1, 0);
        assertTrue(mxBean.getLastCompactionSucceedTimestamp(topic) > 0L);
        assertTrue(mxBean.getLastCompactionFailedTimestamp(topic) > 0L);
        assertTrue(mxBean.getLastCompactionDurationTimeInMills(topic) >= 0L);
    }

}
