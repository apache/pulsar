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
package org.apache.pulsar.broker.topiclistlimit;

import static org.testng.Assert.assertEquals;
import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.client.CollectorRegistry;
import org.testng.annotations.Test;

public class TopicListMemoryLimiterTest {

    @Test
    public void testWaitTimeIsRecordedWithSubMillisecondPrecision() {
        CollectorRegistry registry = new CollectorRegistry();
        try (TopicListMemoryLimiter limiter = new TopicListMemoryLimiter(registry, "test_",
                OpenTelemetry.noop().getMeter("test"), 1024, 10, 1000, 1024, 10, 1000)) {
            limiter.recordHeapWaitTime(1_500_000);
            limiter.recordDirectWaitTime(250_000);

            // truncating to whole milliseconds would record 0 ms for nearly every request, see recordHeapWaitTime
            assertEquals(registry.getSampleValue("test_topic_list_heap_wait_time_ms_sum"), 1.5);
            assertEquals(registry.getSampleValue("test_topic_list_direct_wait_time_ms_sum"), 0.25);
        }
    }
}
