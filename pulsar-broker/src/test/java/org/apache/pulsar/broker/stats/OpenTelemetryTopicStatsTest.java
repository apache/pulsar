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
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryTopicStatsTest extends BrokerTestBase {

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

    @Test(timeOut = 30_000)
    public void testMessagingMetrics() throws Exception {
        var topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testMessagingMetrics");
        admin.topics().createNonPartitionedTopic(topicName);

        var producerCount = 5;
        var messagesPerProducer = 2;
        var consumerCount = 3;
        var messageCount = producerCount * messagesPerProducer;

        for (int i = 0; i < producerCount; i++) {
            var producer = registerCloseable(pulsarClient.newProducer().topic(topicName).create());
            for (int j = 0; j < messagesPerProducer; j++) {
                producer.send(String.format("producer-%d-msg-%d", i, j).getBytes());
            }
        }

        var cdl = new CountDownLatch(consumerCount);
        for (int i = 0; i < consumerCount; i++) {
            var consumer = registerCloseable(pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName("test")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe());
            consumer.receiveAsync().orTimeout(100, TimeUnit.MILLISECONDS).handle((__, ex) -> {
                cdl.countDown();
                return null;
            });
        }
        cdl.await();

        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
                .put(OpenTelemetryAttributes.PULSAR_TENANT, "prop")
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "prop/ns-abc")
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName)
                .build();

        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();

        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.SUBSCRIPTION_COUNTER, attributes, 1);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.PRODUCER_COUNTER, attributes, producerCount);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.CONSUMER_COUNTER, attributes, consumerCount);

        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.MESSAGE_IN_COUNTER, attributes, messageCount);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.MESSAGE_OUT_COUNTER, attributes, messageCount);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.BYTES_IN_COUNTER, attributes,
                actual -> assertThat(actual).isPositive());
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.BYTES_OUT_COUNTER, attributes,
                actual -> assertThat(actual).isPositive());

        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_COUNTER, attributes,
                actual -> assertThat(actual).isPositive());
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_LOGICAL_COUNTER,  attributes,
                actual -> assertThat(actual).isPositive());
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_BACKLOG_COUNTER,  attributes,
                actual -> assertThat(actual).isPositive());

        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_OUT_COUNTER, attributes, messageCount);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_IN_COUNTER, attributes, messageCount);
    }

    @Test(timeOut = 30_000)
    public void testPublishRateLimitMetric() throws Exception {
        var topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testPublishRateLimitMetric");
        admin.topics().createNonPartitionedTopic(topicName);

        var publishRate = new PublishRate(1, -1);
        admin.topicPolicies().setPublishRate(topicName, publishRate);
        Awaitility.await().until(() -> Objects.equals(publishRate, admin.topicPolicies().getPublishRate(topicName)));

        @Cleanup
        var producer = pulsarClient.newProducer().topic(topicName).create();
        producer.send("msg".getBytes());

        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
                .put(OpenTelemetryAttributes.PULSAR_TENANT, "prop")
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "prop/ns-abc")
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName)
                .build();

        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.PUBLISH_RATE_LIMIT_HIT_COUNTER, attributes, 1);
    }
}
