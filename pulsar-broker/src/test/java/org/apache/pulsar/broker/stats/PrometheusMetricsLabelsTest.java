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
package org.apache.pulsar.broker.stats;

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import com.google.common.collect.Multimap;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetricsToken;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class PrometheusMetricsLabelsTest extends BrokerTestBase {

    private static final Set<String> ALLOWED_CUSTOM_METRIC_LABEL_KEYS = Set.of("sla_tier", "app_owner");

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        AuthenticationMetricsToken.reset();
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setExposeCustomTopicMetricLabelsEnabled(true);
        conf.setAllowedTopicPropertiesForMetrics(ALLOWED_CUSTOM_METRIC_LABEL_KEYS);
        // wait for shutdown of the broker, this prevents flakiness which could be caused by metrics being
        // unregistered asynchronously. This impacts the execution of the next test method if this would be happening.
        conf.setBrokerShutdownTimeoutMs(5000L);
        return conf;
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    public void testCustomMetricLabels() throws Exception {
        String topic1 = "persistent://prop/ns-abc/my-topic1";
        String topic2 = "persistent://prop/ns-abc/my-topic2";

        admin.topics().createPartitionedTopic(topic2, 2);

        @Cleanup
        Producer<byte[]> p1 = pulsarClient.newProducer().topic(topic1).create();
        @Cleanup
        Producer<byte[]> p2 = pulsarClient.newProducer().topic(topic2).create();

        @Cleanup
        Consumer<byte[]> c1 = pulsarClient.newConsumer()
            .topic(topic1)
            .subscriptionName("test")
            .subscribe();
        @Cleanup
        Consumer<byte[]> c2 = pulsarClient.newConsumer()
            .topic(topic2)
            .subscriptionName("test")
            .subscribe();

        final int messages = 10;

        for (int i = 0; i < messages; i++) {
            String message = "my-message-" + i;
            p1.send(message.getBytes());
            p2.send(message.getBytes());
        }

        for (int i = 0; i < messages; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        // Set custom metric labels
        Map<String, String> labels = new HashMap<>();
        labels.put("sla_tier", "gold");
        labels.put("app_owner", "team-a");
//        admin.topicPolicies().setCustomMetricLabels(topic1, labels);
        admin.topics().updateProperties(topic1, labels);

        labels = new HashMap<>();
        labels.put("sla_tier", "platinum");
        labels.put("app_owner", "team-b");
//        admin.topicPolicies().setCustomMetricLabels(topic2, labels);
        admin.topics().updateProperties(topic2, labels);

        // Verify labels are set
        Awaitility.await().untilAsserted(() -> {
//            Map<String, String> retrievedLabels = admin.topicPolicies().getCustomMetricLabels(topic1);
            Map<String, String> retrievedLabels = admin.topics().getProperties(topic1);
            assertNotNull(retrievedLabels);
            assertEquals(retrievedLabels.size(), 2);
            assertEquals(retrievedLabels.get("sla_tier"), "gold");
            assertEquals(retrievedLabels.get("app_owner"), "team-a");

            retrievedLabels = admin.topics().getProperties(topic2);
            assertNotNull(retrievedLabels);
            assertEquals(retrievedLabels.size(), 2);
            assertEquals(retrievedLabels.get("sla_tier"), "platinum");
            assertEquals(retrievedLabels.get("app_owner"), "team-b");
        });

        ByteArrayOutputStream statsOut = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, true, false, false, statsOut);
        String metricsStr = statsOut.toString();
        Multimap<String, PrometheusMetricsClient.Metric> metrics = parseMetrics(metricsStr);

        metrics.entries().forEach(e -> {
            System.out.println(e.getKey() + ": " + e.getValue());
        });

        metrics.entries().forEach(entry -> {
            Map<String, String> tags = entry.getValue().tags;
            if (tags.containsKey("topic")) {
                String topic = tags.get("topic");
                if (topic.equals(topic1)) {
                    assertEquals(tags.get("sla_tier"), "gold", "Custom label sla_tier not found for topic1");
                    assertEquals(tags.get("app_owner"), "team-a", "Custom label app_owner not found for topic1");
                } else if (topic.startsWith(topic2)) {
                    assertEquals(tags.get("sla_tier"), "platinum", "Custom label sla_tier not found for topic2");
                    assertEquals(tags.get("app_owner"), "team-b", "Custom label app_owner not found for topic2");
                }
            }
        });
    }
}
