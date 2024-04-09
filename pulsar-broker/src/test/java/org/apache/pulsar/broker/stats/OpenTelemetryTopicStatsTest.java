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

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;

import io.opentelemetry.api.common.Attributes;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
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

    @Test(timeOut = 3000_000)
    public void testMessagingMetrics() throws Exception {
        var topicName = "persistent://prop/ns-abc/testMessagingMetrics";
        var namespace = "prop/ns-abc";

        var backlogTimeLimit = 1;
        var backlogSizeLimit = 1;
        admin.topics().createNonPartitionedTopic(topicName);
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                .limitSize(backlogSizeLimit)
                .limitTime(backlogTimeLimit)
                .build();
        admin.namespaces().setBacklogQuota(namespace, backlogQuota, BacklogQuota.BacklogQuotaType.message_age);
        admin.namespaces().setBacklogQuota(namespace, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            var quota = admin.namespaces().getBacklogQuotaMap(namespace);
            assertThat(quota.get(BacklogQuota.BacklogQuotaType.message_age)).isEqualTo(backlogQuota);
            assertThat(quota.get(BacklogQuota.BacklogQuotaType.destination_storage)).isEqualTo(backlogQuota);
        });

        var producerCount = 5;
        var messagesPerProducer = 2;
        var consumerCount = 3;

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
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe());
            consumer.receiveAsync().orTimeout(100, TimeUnit.MILLISECONDS).handle((msg, ex) -> {
                cdl.countDown();
                return msg;
            });
        }
        cdl.await();

        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
                .put(OpenTelemetryAttributes.PULSAR_TENANT, "prop")
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "ns-abc")
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, "testMessagingMetrics")
                .build();

        var fixmeNilValue = 0L;
        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();

        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.SUBSCRIPTION_COUNTER, 1, attributes);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.PRODUCER_COUNTER, producerCount, attributes);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.CONSUMER_COUNTER, consumerCount, attributes);

        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.MESSAGE_IN_COUNTER,
                producerCount * messagesPerProducer, attributes);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.MESSAGE_OUT_COUNTER, fixmeNilValue, attributes);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.BYTES_IN_COUNTER, 470L, attributes);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.BYTES_OUT_COUNTER, fixmeNilValue, attributes);

        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.PUBLISH_RATE_LIMIT_HIT_COUNTER, fixmeNilValue,
                attributes);

        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_COUNTER, 940L, attributes);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_LOGICAL_COUNTER, 470L, attributes);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_BACKLOG_COUNTER, fixmeNilValue, attributes);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_OFFLOADED_COUNTER, fixmeNilValue, attributes);

        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_OUT_COUNTER,
                producerCount * messagesPerProducer, attributes);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_IN_COUNTER, fixmeNilValue, attributes);
    }
}
