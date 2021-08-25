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
package org.apache.pulsar.broker.stats;

import lombok.Cleanup;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.metrics.CompactionMetrics;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.stats.Metrics;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Random;

@Test(groups = "broker")
public class CompactionMetricsTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testCompactionMetrics() throws Exception {
        pulsar.getCompactor(true);
        CompactionMetrics metrics = new CompactionMetrics(pulsar);

        final String compactionRemovedEventCountKey = "brk_compaction_removedEventCount";
        final String compactionSucceedCountKey = "brk_compaction_succeedCount";
        final String compactionFailedCountKey = "brk_compaction_failedCount";
        final String compactionDurationTimeInMillsKey = "brk_compaction_durationTimeInMills";
        final String compactionReadThroughputKey = "brk_compaction_readThroughput";
        final String compactionWriteThroughputKey = "brk_compaction_writeThroughput";
        List<Metrics> list1 = metrics.generate();
        Assert.assertTrue(list1.isEmpty());

        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 1000;
        final int maxKeys = 10;

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Random r = new Random(0);

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            byte[] data = ("my-message-" + key + "-" + j).getBytes();
            producer.newMessage()
                    .key(key)
                    .value(data)
                    .send();
        }

        pulsar.getCompactor(true).compact(topic).get();
        List<Metrics> list2 = metrics.generate();
        Assert.assertEquals(list2.get(0).getMetrics().get(compactionRemovedEventCountKey), 990.0D);
        Assert.assertEquals(list2.get(0).getMetrics().get(compactionSucceedCountKey), 1.0);
        Assert.assertEquals(list2.get(0).getMetrics().get(compactionFailedCountKey), 0.0);
        Assert.assertTrue((Double)list2.get(0).getMetrics().get(compactionDurationTimeInMillsKey) >= 0.0D);
        Assert.assertTrue((Double)list2.get(0).getMetrics().get(compactionReadThroughputKey) >= 0.0D);
        Assert.assertTrue((Double)list2.get(0).getMetrics().get(compactionWriteThroughputKey) >= 0.0D);
    }
}
