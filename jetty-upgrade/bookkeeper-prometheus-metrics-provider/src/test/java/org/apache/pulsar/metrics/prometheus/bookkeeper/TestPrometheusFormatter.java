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

import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;
import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.hotspot.GarbageCollectorExports;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import io.prometheus.client.hotspot.StandardExports;
import io.prometheus.client.hotspot.ThreadExports;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.testng.annotations.Test;

/**
 * Test for {@link PrometheusMetricsProvider}.
 */
public class TestPrometheusFormatter {

    @Test(timeOut = 30000)
    public void testStatsOutput() throws Exception {
        PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
        StatsLogger statsLogger = provider.getStatsLogger("test");
        Counter counter = statsLogger.getCounter("my_counter");

        counter.inc();
        counter.inc();

        OpStatsLogger opStats = statsLogger.getOpStatsLogger("op");
        opStats.registerSuccessfulEvent(10, TimeUnit.MILLISECONDS);
        opStats.registerSuccessfulEvent(5, TimeUnit.MILLISECONDS);

        OpStatsLogger opStats1 = statsLogger.scopeLabel("test_label", "test_value")
                .getOpStatsLogger("op_label");
        opStats1.registerSuccessfulEvent(10, TimeUnit.MILLISECONDS);
        opStats1.registerSuccessfulEvent(5, TimeUnit.MILLISECONDS);
        opStats1.registerFailedEvent(1, TimeUnit.MILLISECONDS);

        provider.rotateLatencyCollection();

        StringWriter writer = new StringWriter();
        provider.writeAllMetrics(writer);

        writer.write("jvm_memory_direct_bytes_max{} 4.77626368E8\n");
        writer.write("jvm_memory_pool_bytes_used{pool=\"Code Cache\"} 3347712.0\n");
        writer.write("jvm_memory_pool_bytes_used{pool=\"CodeHeap 'non-nmethods'\"} 1207168.0\n");
        System.out.println(writer);
        Multimap<String, Metric> metrics = parseMetrics(writer.toString());
        System.out.println(metrics);

        List<Metric> cm = (List<Metric>) metrics.get("test_my_counter");
        assertEquals(1, cm.size());
        assertEquals(0, cm.get(0).tags.size());
        assertEquals(2.0, cm.get(0).value, 0.0);

        // test_op_sum
        cm = (List<Metric>) metrics.get("test_op_sum");
        assertEquals(2, cm.size());
        Metric m = cm.get(0);
        assertEquals(1, cm.get(0).tags.size());
        assertEquals(0.0, m.value, 0.0);
        assertEquals(1, m.tags.size());
        assertEquals("false", m.tags.get("success"));

        m = cm.get(1);
        assertEquals(1, cm.get(0).tags.size());
        assertEquals(15.0, m.value, 0.0);
        assertEquals(1, m.tags.size());
        assertEquals("true", m.tags.get("success"));

        // test_op_count
        cm = (List<Metric>) metrics.get("test_op_count");
        assertEquals(2, cm.size());
        m = cm.get(0);
        assertEquals(1, cm.get(0).tags.size());
        assertEquals(0.0, m.value, 0.0);
        assertEquals(1, m.tags.size());
        assertEquals("false", m.tags.get("success"));

        m = cm.get(1);
        assertEquals(1, cm.get(0).tags.size());
        assertEquals(2.0, m.value, 0.0);
        assertEquals(1, m.tags.size());
        assertEquals("true", m.tags.get("success"));

        // Latency
        cm = (List<Metric>) metrics.get("test_op");
        assertEquals(14, cm.size());

        boolean found = false;
        for (Metric mt  : cm) {
            if ("true".equals(mt.tags.get("success")) && "1.0".equals(mt.tags.get("quantile"))) {
                assertEquals(10.0, mt.value, 0.0);
                found = true;
            }
        }

        assertTrue(found);

        // test_op_label_sum
        cm = (List<Metric>) metrics.get("test_op_label_sum");
        assertEquals(2, cm.size());
        m = cm.get(0);
        assertEquals(2, m.tags.size());
        assertEquals(1.0, m.value, 0.0);
        assertEquals("false", m.tags.get("success"));
        assertEquals("test_value", m.tags.get("test_label"));

        m = cm.get(1);
        assertEquals(15.0, m.value, 0.0);
        assertEquals(2, m.tags.size());
        assertEquals("true", m.tags.get("success"));
        assertEquals("test_value", m.tags.get("test_label"));

        // test_op_label_count
        cm = (List<Metric>) metrics.get("test_op_label_count");
        assertEquals(2, cm.size());
        m = cm.get(0);
        assertEquals(1, m.value, 0.0);
        assertEquals(2, m.tags.size());
        assertEquals("false", m.tags.get("success"));
        assertEquals("test_value", m.tags.get("test_label"));

        m = cm.get(1);
        assertEquals(2.0, m.value, 0.0);
        assertEquals(2, m.tags.size());
        assertEquals("true", m.tags.get("success"));
        assertEquals("test_value", m.tags.get("test_label"));

        // Latency
        cm = (List<Metric>) metrics.get("test_op_label");
        assertEquals(14, cm.size());

        found = false;
        for (Metric mt : cm) {
            if ("true".equals(mt.tags.get("success"))
                    && "test_value".equals(mt.tags.get("test_label"))
                    && "1.0".equals(mt.tags.get("quantile"))) {
                assertEquals(10.0, mt.value, 0.0);
                found = true;
            }
        }

        assertTrue(found);
    }

    @Test
    public void testWriteMetricsCollectedByPrometheusClient() {
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        registry.register(new StandardExports());
        registry.register(new MemoryPoolsExports());
        registry.register(new GarbageCollectorExports());
        registry.register(new ThreadExports());
        registry.register(Gauge.build("jvm_memory_direct_bytes_used", "-").create().setChild(new Gauge.Child() {
            @Override
            public double get() {
                return 1.0;
            }
        }));
        registry.register(Gauge.build("jvm_memory_direct_bytes_max", "-").create().setChild(new Gauge.Child() {
            @Override
            public double get() {
                return 100.0;
            }
        }));
        PrometheusMetricsProvider provider = new PrometheusMetricsProvider(registry);
        StringWriter writer = new StringWriter();
        try {
            provider.rotateLatencyCollection();
            provider.writeAllMetrics(writer);
            String output = writer.toString();
            parseMetrics(output);
            assertTrue(output.contains("# TYPE jvm_memory_direct_bytes_max gauge"));
            assertTrue(output.contains("# TYPE jvm_memory_direct_bytes_used gauge"));
            assertTrue(output.contains("# TYPE jvm_gc_collection_seconds summary"));
            assertTrue(output.contains("# TYPE jvm_memory_pool_bytes_committed gauge"));
            assertTrue(output.contains("# TYPE process_cpu_seconds counter"));
        } catch (Exception e) {
            fail();
        }

    }

    @Test
    public void testPrometheusTypeDuplicate() throws IOException {
        PrometheusTextFormat prometheusTextFormat = new PrometheusTextFormat();
        StringWriter writer = new StringWriter();
        prometheusTextFormat.writeType(writer, "counter", "gauge");
        prometheusTextFormat.writeType(writer, "counter", "gauge");
        String string = writer.toString();
        assertEquals("# TYPE counter gauge\n", string);
    }


    /**
     * Hacky parsing of Prometheus text format. Sould be good enough for unit tests
     */
    private static Multimap<String, Metric> parseMetrics(String metrics) {
        Multimap<String, Metric> parsed = ArrayListMultimap.create();

        // Example of lines are
        // jvm_threads_current{cluster="standalone",} 203.0
        // or
        // pulsar_subscriptions_count{cluster="standalone", namespace="sample/standalone/ns1",
        // topic="persistent://sample/standalone/ns1/test-2"} 0.0 1517945780897
        Pattern pattern = Pattern.compile("^(\\w+)(\\{([^\\}]*)\\})?\\s(-?[\\d\\w\\.]+)(\\s(\\d+))?$");
        Pattern formatPattern =
                Pattern.compile("^(\\w+)(\\{((\\w+=[-\\s\\\'\\\"\\.\\w]+(,\\s?\\w+=[\\\"\\.\\w]+)*))?\\})?"
                        + "\\s(-?[\\d\\w\\.]+)(\\s(\\d+))?$");
        Pattern tagsPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"(,\\s?)?");

        Splitter.on("\n").split(metrics).forEach(line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }

            System.err.println("LINE: '" + line + "'");
            Matcher matcher = pattern.matcher(line);
            Matcher formatMatcher = formatPattern.matcher(line);
            System.err.println("Matches: " + matcher.matches());
            System.err.println(matcher);
            assertTrue(matcher.matches());
            assertTrue("failed to validate line: " + line, formatMatcher.matches());

            assertEquals(6, matcher.groupCount());
            System.err.println("groups: " + matcher.groupCount());
            for (int i = 0; i < matcher.groupCount(); i++) {
                System.err.println("   GROUP " + i + " -- " + matcher.group(i));
            }

            checkArgument(matcher.matches());
            checkArgument(formatMatcher.matches());
            String name = matcher.group(1);

            Metric m = new Metric();
            m.value = Double.parseDouble(matcher.group(4));

            String tags = matcher.group(3);
            if (tags != null) {
                Matcher tagsMatcher = tagsPattern.matcher(tags);
                while (tagsMatcher.find()) {
                    String tag = tagsMatcher.group(1);
                    String value = tagsMatcher.group(2);
                    m.tags.put(tag, value);
                }
            }

            parsed.put(name, m);
        });

        return parsed;
    }

    static class Metric {
        Map<String, String> tags = new TreeMap<>();
        double value;

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("tags", tags).add("value", value).toString();
        }
    }
}
