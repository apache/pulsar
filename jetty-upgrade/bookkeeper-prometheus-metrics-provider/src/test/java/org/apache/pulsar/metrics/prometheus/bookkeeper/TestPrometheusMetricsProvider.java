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
package org.apache.pulsar.metrics.prometheus.bookkeeper;

import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.testng.annotations.Test;

/**
 * Unit test of {@link PrometheusMetricsProvider}.
 */
public class TestPrometheusMetricsProvider {

    @Test
    public void testStartNoHttp() {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
        try {
            provider.start(config);
            assertNull(provider.server);
        } finally {
            provider.stop();
        }
    }

    @Test
    public void testStartNoHttpWhenBkHttpEnabled() {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, true);
        config.setProperty("httpServerEnabled", true);
        @Cleanup("stop") PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
        provider.start(config);
        assertNull(provider.server);
    }

    @Test
    public void testStartWithHttp() {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, true);
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_PORT, 0); // ephemeral
        PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
        try {
            provider.start(config);
            assertNotNull(provider.server);
        } finally {
            provider.stop();
        }
    }

    @Test
    public void testStartWithHttpSpecifyAddr() {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, true);
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_PORT, 0); // ephemeral
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ADDRESS, "127.0.0.1");
        PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
        try {
            provider.start(config);
            assertNotNull(provider.server);
        } finally {
            provider.stop();
        }
    }

    @Test
    public void testCounter() {
        LongAdderCounter counter = new LongAdderCounter(Collections.emptyMap());
        long value = counter.get();
        assertEquals(0L, value);
        counter.inc();
        assertEquals(1L, counter.get().longValue());
        counter.dec();
        assertEquals(0L, counter.get().longValue());
        counter.addCount(3);
        assertEquals(3L, counter.get().longValue());
    }

    @Test
    public void testCounter2() {
        LongAdderCounter counter = new LongAdderCounter(Collections.emptyMap());
        long value = counter.get();
        assertEquals(0L, value);
        counter.addLatency(3 * 1000 * 1000L, TimeUnit.NANOSECONDS);
        assertEquals(3L, counter.get().longValue());
    }

    @Test
    public void testTwoCounters() throws Exception {
        PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
        StatsLogger statsLogger =  provider.getStatsLogger("test");

        Counter counter1 = statsLogger.getCounter("counter");
        Counter counter2 = statsLogger.getCounter("counter");
        assertEquals(counter1, counter2);
        assertSame(counter1, counter2);

        assertEquals(1, provider.counters.size());
    }

    @Test
    public void testJvmDirectMemoryMetrics() throws Exception {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, true);
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_PORT, 0);
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ADDRESS, "127.0.0.1");
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.directBuffer(25);
        PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
        try {
            provider.start(config);
            assertNotNull(provider.server);
            StringWriter writer = new StringWriter();
            provider.writeAllMetrics(writer);
            String s = writer.toString();
            String[] split = s.split(System.lineSeparator());
            HashMap<String, String> map = new HashMap<>();
            for (String str : split) {
                String[] aux = str.split(" ");
                map.put(aux[0], aux[1]);
            }
            String directBytesMax = map.get("jvm_memory_direct_bytes_max{}");
            assertNotNull(directBytesMax);
            assertNotEquals("Nan", directBytesMax);
            assertNotEquals("-1", directBytesMax);
            String directBytesUsed = map.get("jvm_memory_direct_bytes_used{}");
            assertNotNull(directBytesUsed);
            assertNotEquals("Nan", directBytesUsed);
            // this condition is flaky
            //assertTrue(Double.parseDouble(directBytesUsed) > 25);
            // ensure byteBuffer doesn't gc
            byteBuf.release();
        } finally {
            provider.stop();
        }
    }

}
