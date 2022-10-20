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

import com.google.common.collect.Multimap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pulsar.broker.stats.prometheus.metrics.PrometheusMetricsProvider;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.Test;
import static org.apache.pulsar.broker.stats.PrometheusMetricsTest.parseMetrics;
import static org.apache.pulsar.broker.stats.PrometheusMetricsTest.Metric;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class CachingStatsLoggerTest {

    @Test
    public void testSameScopeLabelShareSameInstance() throws IOException {
        PrometheusMetricsProvider metricsProvider = new PrometheusMetricsProvider();
        try {
            metricsProvider.start(new PropertiesConfiguration());
            StatsLogger statsLogger = metricsProvider.getStatsLogger("test_raw");
            StatsLogger topicStatsLogger = statsLogger.scopeLabel("topic", "persistent://public/default/test-v1");
            StatsLogger t1 = statsLogger.scopeLabel("topic", "persistent://public/default/test-v2");
            StatsLogger t2 = topicStatsLogger.scopeLabel("aa", "aa-value");
            StatsLogger t3 = statsLogger.scopeLabel("topic", "persistent://public/default/test-v2");
            StatsLogger t4 = statsLogger.scopeLabel("topic", "persistent://public/default/test-v2");
            StatsLogger t5 = topicStatsLogger.scopeLabel("aa", "aa-value");

            assertEquals(t3, t1);
            assertEquals(t4, t1);
            assertEquals(t5, t2);
            assertNotEquals(t5, t4);

            StatsLogger t6 = statsLogger.scope("test");
            StatsLogger t7 = statsLogger.scope("test");
            StatsLogger t8 = statsLogger.scope("test_v1");

            assertEquals(t7, t6);
            assertNotEquals(t8, t6);

            // check OpStatsLogger with one label
            OpStatsLogger op1 = t1.getOpStatsLogger("op");
            OpStatsLogger op2 = t3.getOpStatsLogger("op");
            OpStatsLogger op3 = t4.getOpStatsLogger("op");
            OpStatsLogger op4 = t4.getOpStatsLogger("op4");
            assertEquals(op2, op1);
            assertEquals(op3, op1);
            assertNotEquals(op4, op1);
            assertNotEquals(t1, op1);

            // check counter with one label
            Counter c1 = t1.getCounter("counter");
            Counter c2 = t3.getCounter("counter");
            Counter c3 = t4.getCounter("counter");
            Counter c4 = t4.getCounter("counter4");
            assertEquals(c2, c1);
            assertEquals(c3, c1);
            assertNotEquals(c4, c1);
            assertNotEquals(t1, c1);

            // check OpStatsLogger with one label
            OpStatsLogger mop1 = t2.getOpStatsLogger("op");
            OpStatsLogger mop2 = t5.getOpStatsLogger("op");
            OpStatsLogger mop3 = t5.getOpStatsLogger("op3");
            assertEquals(mop2, mop1);
            assertNotEquals(mop3, mop1);
            assertNotEquals(op1, mop1);
            assertNotEquals(t1, mop1);

            // check counter with one label
            Counter mc1 = t2.getCounter("counter");
            Counter mc2 = t5.getCounter("counter");
            Counter mc3 = t5.getCounter("counter3");
            assertEquals(mc2, mc1);
            assertNotEquals(mc3, mc1);
            assertNotEquals(c1, mc1);
            assertNotEquals(t1, mc1);

            topicStatsLogger.getOpStatsLogger("writeLatency").registerSuccessfulEvent(100, TimeUnit.NANOSECONDS);
            topicStatsLogger.getOpStatsLogger("write_v1").registerSuccessfulEvent(200, TimeUnit.NANOSECONDS);
            t1.getOpStatsLogger("write_v2").registerSuccessfulEvent(300, TimeUnit.NANOSECONDS);
            t1.getOpStatsLogger("write_v3").registerSuccessfulEvent(400, TimeUnit.NANOSECONDS);
            t2.getOpStatsLogger("write_4").registerSuccessfulEvent(500, TimeUnit.NANOSECONDS);
            t3.getOpStatsLogger("write_v5").registerSuccessfulEvent(600, TimeUnit.NANOSECONDS);
            t5.getOpStatsLogger("write_v5").registerSuccessfulEvent(600, TimeUnit.NANOSECONDS);
            t6.getOpStatsLogger("write_v5").registerSuccessfulEvent(600, TimeUnit.NANOSECONDS);
            t7.getOpStatsLogger("write_v5").registerSuccessfulEvent(600, TimeUnit.NANOSECONDS);
            t8.getOpStatsLogger("write_v5").registerSuccessfulEvent(600, TimeUnit.NANOSECONDS);

            ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);
            metricsProvider.generate(stream);
            Multimap<String, Metric> metrics = parseMetrics(stream.getBuffer().toString(StandardCharsets.UTF_8));

            assertEquals(((List<Metric>) metrics.get("test_raw_counter4")).get(0).tags.get("topic"),
                "persistent://public/default/test-v2");
            Metric metric = ((List<Metric>) metrics.get("test_raw_counter3")).get(0);
            assertEquals(metric.tags.get("topic"), "persistent://public/default/test-v1");
            assertEquals(metric.tags.get("aa"), "aa-value");

            List<Metric> metricList = ((List<Metric>) metrics.get("test_raw_test_v1_write_v5_count"));
            if (metricList.get(0).tags.get("success").equals("false")) {
                assertEquals(metricList.get(0).value, 0.0);
                assertEquals(metricList.get(1).value, 1.0);
            } else {
                assertEquals(metricList.get(0).value, 1.0);
                assertEquals(metricList.get(1).value, 0.0);
            }

            metricList = ((List<Metric>) metrics.get("test_raw_test_write_v5_count"));
            if (metricList.get(0).tags.get("success").equals("false")) {
                assertEquals(metricList.get(0).value, 0.0);
                assertEquals(metricList.get(1).value, 2.0);
            } else {
                assertEquals(metricList.get(0).value, 2.0);
                assertEquals(metricList.get(1).value, 0.0);
            }
        } finally {
            metricsProvider.stop();
        }
    }
}
