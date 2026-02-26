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

import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.common.Attributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class OpenTelemetryCustomLabelsTest extends BrokerTestBase {

    private static final Set<String> ALLOWED_CUSTOM_METRIC_LABEL_KEYS = Set.of("__SLA_TIER", "APP_OWNER");

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder builder) {
        super.customizeMainPulsarTestContextBuilder(builder);
        builder.enableOpenTelemetry(true);
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setExposeCustomTopicMetricLabelsEnabled(true);
        conf.setAllowedTopicPropertiesForMetrics(ALLOWED_CUSTOM_METRIC_LABEL_KEYS);
        conf.setBrokerShutdownTimeoutMs(5000L);
        return conf;
    }

    @Test(timeOut = 30_000)
    public void testCustomLabelsInOpenTelemetryMetrics() throws Exception {
        var topic1 = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testCustomLabels1");
        var topic2 = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testCustomLabels2");

        admin.topics().createNonPartitionedTopic(topic1);
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

        // Produce and consume messages
        for (int i = 0; i < 5; i++) {
            p1.send(("message-" + i).getBytes());
            p2.send(("message-" + i).getBytes());
        }
        for (int i = 0; i < 5; i++) {
            c1.acknowledge(c1.receive());
            c2.acknowledge(c2.receive());
        }

        // Set custom metric labels for topic1
        Map<String, String> labels1 = new HashMap<>();
        labels1.put("SLA_TIER", "gold");
        labels1.put("APP_OWNER", "team-a");
        admin.topics().updateProperties(topic1, labels1);

        // Set custom metric labels for topic2
        Map<String, String> labels2 = new HashMap<>();
        labels2.put("SLA_TIER", "platinum");
        labels2.put("APP_OWNER", "team-b");
        admin.topics().updateProperties(topic2, labels2);

        // Wait for labels to be set
        Awaitility.await().untilAsserted(() -> {
            var retrievedLabels1 = admin.topics().getProperties(topic1);
            assertThat(retrievedLabels1.get("sla_tier")).isEqualTo("gold");
            assertThat(retrievedLabels1.get("app_owner")).isEqualTo("team-a");

            var retrievedLabels2 = admin.topics().getProperties(topic2);
            assertThat(retrievedLabels2.get("sla_tier")).isEqualTo("platinum");
            assertThat(retrievedLabels2.get("app_owner")).isEqualTo("team-b");
        });

        // Collect metrics and verify custom labels are present
        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();

        // Build expected attributes for topic1 with custom labels
        var attributesTopic1 = Attributes.builder()
            .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
            .put(OpenTelemetryAttributes.PULSAR_TENANT, "prop")
            .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "prop/ns-abc")
            .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic1)
            .put("sla_tier", "gold")
            .put("app_owner", "team-a")
            .build();

        // Build expected attributes for topic2 with custom labels
        var attributesTopic2 = Attributes.builder()
            .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
            .put(OpenTelemetryAttributes.PULSAR_TENANT, "prop")
            .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "prop/ns-abc")
            .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic2)
            .put("sla_tier", "platinum")
            .put("app_owner", "team-b")
            .build();

        // Verify metrics contain custom labels for topic1
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.MESSAGE_IN_COUNTER, attributesTopic1, 5);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.PRODUCER_COUNTER, attributesTopic1, 1);

        // Verify metrics contain custom labels for topic2 (partitioned topic)
        // For partitioned topics, metrics are reported per partition
        boolean foundTopic2Metrics = metrics.stream()
            .filter(metric -> metric.getName().equals(OpenTelemetryTopicStats.MESSAGE_IN_COUNTER))
            .flatMap(metric -> metric.getLongSumData().getPoints().stream())
            .anyMatch(point -> {
                var attrs = point.getAttributes();
                return attrs.get(OpenTelemetryAttributes.PULSAR_TOPIC).equals(topic2)
                    && "platinum".equals(attrs.get(io.opentelemetry.api.common.AttributeKey
                    .stringKey("sla_tier")))
                    && "team-b".equals(attrs.get(io.opentelemetry.api.common.AttributeKey
                    .stringKey("app_owner")));
            });
        assertThat(foundTopic2Metrics).isTrue();
    }

    @Test(timeOut = 30_000)
    public void testCustomLabelsDisabled() throws Exception {
        // Create a new test with custom labels disabled
        var topic = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testCustomLabelsDisabled");
        admin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        producer.send("test".getBytes());

        // Set custom metric labels
        Map<String, String> labels = new HashMap<>();
        labels.put("SLA_TIER", "gold");
        admin.topics().updateProperties(topic, labels);

        // Wait for labels to be set
        Awaitility.await().untilAsserted(() -> {
            var retrievedLabels = admin.topics().getProperties(topic);
            assertThat(retrievedLabels.get("sla_tier")).isEqualTo("gold");
        });

        // Temporarily disable custom labels
        pulsar.getConfiguration().setExposeCustomTopicMetricLabelsEnabled(false);

        try {
            // Collect metrics
            var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();

            // Build expected attributes without custom labels
            var attributesWithoutCustomLabels = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
                .put(OpenTelemetryAttributes.PULSAR_TENANT, "prop")
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "prop/ns-abc")
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic)
                .build();

            // Verify metrics do NOT contain custom labels
            assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.MESSAGE_IN_COUNTER, attributesWithoutCustomLabels,
                1);

            // Verify no metrics contain the custom label
            boolean foundCustomLabel = metrics.stream()
                .filter(metric -> metric.getName().equals(OpenTelemetryTopicStats.MESSAGE_IN_COUNTER))
                .flatMap(metric -> metric.getLongSumData().getPoints().stream())
                .anyMatch(point -> point.getAttributes()
                    .get(io.opentelemetry.api.common.AttributeKey.stringKey("sla_tier")) != null);
            assertThat(foundCustomLabel).isFalse();
        } finally {
            // Re-enable custom labels for other tests
            pulsar.getConfiguration().setExposeCustomTopicMetricLabelsEnabled(true);
        }
    }
}
