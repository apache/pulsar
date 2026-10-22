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
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryConsumerStatsTest extends BrokerTestBase {

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
        var topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testConsumerMessagingMetrics");
        admin.topics().createNonPartitionedTopic(topicName);

        var messageCount = 5;
        var ackCount = 3;

        var subscriptionName = BrokerTestUtil.newUniqueName("test");
        var receiverQueueSize = 100;

        @Cleanup
        var consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .receiverQueueSize(receiverQueueSize)
                .subscribe();

        @Cleanup
        var producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        for (int i = 0; i < messageCount; i++) {
            producer.send(String.format("msg-%d", i).getBytes());
            var message = consumer.receive();
            if (i < ackCount) {
                consumer.acknowledge(message);
            }
        }

        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
                .put(OpenTelemetryAttributes.PULSAR_TENANT, "prop")
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "prop/ns-abc")
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName)
                .put(OpenTelemetryAttributes.PULSAR_SUBSCRIPTION_NAME, subscriptionName)
                .put(OpenTelemetryAttributes.PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.Shared.toString())
                .put(OpenTelemetryAttributes.PULSAR_CONSUMER_NAME, consumer.getConsumerName())
                .put(OpenTelemetryAttributes.PULSAR_CONSUMER_ID, 0)
                .build();

        Awaitility.await().untilAsserted(() -> {
            var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();

            assertMetricLongSumValue(metrics, OpenTelemetryConsumerStats.MESSAGE_OUT_COUNTER, attributes,
                    actual -> assertThat(actual).isPositive());
            assertMetricLongSumValue(metrics, OpenTelemetryConsumerStats.BYTES_OUT_COUNTER, attributes,
                    actual -> assertThat(actual).isPositive());

            assertMetricLongSumValue(metrics, OpenTelemetryConsumerStats.MESSAGE_ACK_COUNTER, attributes, ackCount);
            assertMetricLongSumValue(metrics, OpenTelemetryConsumerStats.MESSAGE_PERMITS_COUNTER, attributes,
                    actual -> assertThat(actual).isGreaterThanOrEqualTo(receiverQueueSize - messageCount - ackCount));

            var unAckCount = messageCount - ackCount;
            assertMetricLongSumValue(metrics, OpenTelemetryConsumerStats.MESSAGE_UNACKNOWLEDGED_COUNTER, attributes,
                    unAckCount);
            assertMetricLongSumValue(metrics, OpenTelemetryConsumerStats.CONSUMER_BLOCKED_COUNTER, attributes, 0);
            assertMetricLongSumValue(metrics, OpenTelemetryConsumerStats.MESSAGE_REDELIVER_COUNTER, attributes,
                    actual -> assertThat(actual).isGreaterThanOrEqualTo(unAckCount));
        });
    }
}
