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

import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.api.Producer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class PrometheusMetricsTest extends BrokerTestBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPerTopicStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, true, false, statsOut);
        String metricsStr = new String(statsOut.toByteArray());

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 2 metrics with different tags for each topic
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_storage_write_latency_le_1");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_producers_count");
        assertEquals(cm.size(), 2);
        assertEquals(cm.get(0).value, 1.0);
        assertEquals(cm.get(0).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic2");
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");
        assertEquals(cm.get(1).value, 1.0);
        assertEquals(cm.get(1).tags.get("topic"), "persistent://my-property/use/my-ns/my-topic1");
        assertEquals(cm.get(1).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("topic_load_times_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 2.0);
        assertEquals(cm.get(0).tags.get("cluster"), "test");

        p1.close();
        p2.close();
    }

    @Test
    public void testPerNamespaceStats() throws Exception {
        Producer<byte[]> p1 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
        Producer<byte[]> p2 = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic2").create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, statsOut);
        String metricsStr = new String(statsOut.toByteArray());

        Multimap<String, Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        // There should be 1 metric aggregated per namespace
        List<Metric> cm = (List<Metric>) metrics.get("pulsar_storage_write_latency_le_1");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).tags.get("topic"), null);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        cm = (List<Metric>) metrics.get("pulsar_producers_count");
        assertEquals(cm.size(), 1);
        assertEquals(cm.get(0).value, 2.0);
        assertEquals(cm.get(0).tags.get("topic"), null);
        assertEquals(cm.get(0).tags.get("namespace"), "my-property/use/my-ns");

        p1.close();
        p2.close();
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
        Pattern pattern = Pattern.compile("^(\\w+)\\{([^\\}]+)\\}\\s(-?[\\d\\w\\.]+)(\\s(\\d+))?$");
        Pattern tagsPattern = Pattern.compile("(\\w+)=\"([^\"]+)\"(,\\s?)?");

        Splitter.on("\n").split(metrics).forEach(line -> {
            if (line.isEmpty() || line.startsWith("#")) {
                return;
            }

            Matcher matcher = pattern.matcher(line);

            checkArgument(matcher.matches());
            String name = matcher.group(1);

            Metric m = new Metric();
            m.value = Double.valueOf(matcher.group(3));

            String tags = matcher.group(2);
            Matcher tagsMatcher = tagsPattern.matcher(tags);
            while (tagsMatcher.find()) {
                String tag = tagsMatcher.group(1);
                String value = tagsMatcher.group(2);
                m.tags.put(tag, value);
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
