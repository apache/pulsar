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
package org.apache.pulsar.broker.delayed.bucket;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.Map;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.testng.annotations.Test;

public class BucketDelayedMessageIndexStatsTest {

    private static final String OP_COUNT = BucketDelayedMessageIndexStats.OP_COUNT_NAME;
    private static final String OP_LATENCY_COUNT = BucketDelayedMessageIndexStats.OP_LATENCY_NAME + "_count";
    private static final String OP_LATENCY_SUM = BucketDelayedMessageIndexStats.OP_LATENCY_NAME + "_sum";

    @Test
    public void testMetricsAreCumulativeAcrossScrapes() {
        BucketDelayedMessageIndexStats stats = new BucketDelayedMessageIndexStats();
        var type = BucketDelayedMessageIndexStats.Type.create;

        stats.recordTriggerEvent(type);
        stats.recordSuccessEvent(type, 200);
        stats.recordSuccessEvent(type, 300);

        // First scrape
        var m1 = stats.genTopicMetricMap();
        assertEquals(get(m1, OP_COUNT, "state", "succeed", "type", "create"), 2);
        assertEquals(get(m1, OP_LATENCY_COUNT, "type", "create"), 2);
        assertEquals(get(m1, OP_LATENCY_SUM, "type", "create"), 500);

        // Second scrape — must not reset
        var m2 = stats.genTopicMetricMap();
        assertEquals(get(m2, OP_COUNT, "state", "succeed", "type", "create"), 2);
        assertEquals(get(m2, OP_LATENCY_COUNT, "type", "create"), 2);
        assertEquals(get(m2, OP_LATENCY_SUM, "type", "create"), 500);

        // Third event — cumulative
        stats.recordSuccessEvent(type, 100);
        var m3 = stats.genTopicMetricMap();
        assertEquals(get(m3, OP_COUNT, "state", "succeed", "type", "create"), 3);
        assertEquals(get(m3, OP_LATENCY_COUNT, "type", "create"), 3);
        assertEquals(get(m3, OP_LATENCY_SUM, "type", "create"), 600);
    }

    private static long get(Map<String, TopicMetricBean> metrics, String name, String... labelsAndValues) {
        String key = name + BucketDelayedMessageIndexStats.joinKey(labelsAndValues);
        TopicMetricBean bean = metrics.get(key);
        assertNotNull(bean, "Metric not found: " + key);
        return (long) bean.value;
    }
}
