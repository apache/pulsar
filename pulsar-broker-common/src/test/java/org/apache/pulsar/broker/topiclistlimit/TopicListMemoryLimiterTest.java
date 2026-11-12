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
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import java.util.Set;
import java.util.TreeSet;
import org.testng.annotations.Test;

public class TopicListMemoryLimiterTest {
    private static final String PREFIX = "limiter_test_";
    private static final Set<String> FAMILIES = Set.of(
            PREFIX + "topic_list_heap_memory_used_bytes",
            PREFIX + "topic_list_heap_memory_limit_bytes",
            PREFIX + "topic_list_direct_memory_used_bytes",
            PREFIX + "topic_list_direct_memory_limit_bytes",
            PREFIX + "topic_list_heap_queue_size",
            PREFIX + "topic_list_heap_queue_max_size",
            PREFIX + "topic_list_direct_queue_size",
            PREFIX + "topic_list_direct_queue_max_size",
            PREFIX + "topic_list_heap_wait_time_ms",
            PREFIX + "topic_list_direct_wait_time_ms",
            PREFIX + "topic_list_heap_timeout",
            PREFIX + "topic_list_direct_timeout");

    /**
     * Two brokers in one JVM create two limiters with the same prefix. The second one loses every registration,
     * so closing it must leave the first one's families in place.
     */
    @Test
    public void testCloseUnregistersOnlyTheCollectorsTheInstanceRegistered() {
        PrometheusRegistry registry = new PrometheusRegistry();
        TopicListMemoryLimiter first = newLimiter(registry);
        TopicListMemoryLimiter second = newLimiter(registry);
        try {
            assertEquals(families(registry), new TreeSet<>(FAMILIES));

            second.close();
            assertEquals(families(registry), new TreeSet<>(FAMILIES));

            first.close();
            assertEquals(families(registry), new TreeSet<>());
        } finally {
            second.close();
            first.close();
        }
    }

    private static TopicListMemoryLimiter newLimiter(PrometheusRegistry registry) {
        return new TopicListMemoryLimiter(registry, PREFIX, OpenTelemetry.noop().getMeter("test"),
                1024, 10, 1000, 1024, 10, 1000);
    }

    private static Set<String> families(PrometheusRegistry registry) {
        Set<String> names = new TreeSet<>();
        for (MetricSnapshot snapshot : registry.scrape()) {
            names.add(snapshot.getMetadata().getPrometheusName());
        }
        return names;
    }
}
