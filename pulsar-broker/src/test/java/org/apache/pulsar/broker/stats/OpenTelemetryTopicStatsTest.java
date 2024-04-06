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

import io.opentelemetry.api.common.Attributes;
import java.util.Optional;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
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

    @Test
    public void testMessagingMetrics() throws Exception {
        var topicName = "persistent://prop/ns-abc/testMessagingMetrics";
        var partitionedTopicName = "persistent://prop/ns-abc/testPartitionedMessagingMetrics";

        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createPartitionedTopic(partitionedTopicName, 3);

        var producerCount = 3;
        var consumerCount = 5;

        for (int i = 0; i < producerCount; i++) {
            registerCloseable(pulsarClient.newProducer().topic(topicName).create());
            registerCloseable(pulsarClient.newProducer().topic(partitionedTopicName).create());
        }

        for (int i = 0; i < consumerCount; i++) {
            registerCloseable(pulsarClient.newConsumer().topic(topicName)
                    .subscriptionName("test")
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe());
        }

        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_DOMAIN, "persistent")
                .put(OpenTelemetryAttributes.PULSAR_TENANT, "prop")
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, "ns-abc")
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, "testMessagingMetrics")
                .build();

        var dummyValue = 1000;

        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();

        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.SUBSCRIPTION_COUNTER, 1, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.PRODUCER_COUNTER, producerCount, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.CONSUMER_COUNTER, consumerCount, attributes);

        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.MESSAGE_IN_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.MESSAGE_OUT_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.BYTES_IN_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.BYTES_OUT_COUNTER, dummyValue, attributes);

        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.PUBLISH_RATE_LIMIT_HIT_COUNTER, dummyValue, attributes);
        // assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.CONSUMER_MSG_ACK_COUNTER, dummyValue, attributes); is missing

        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_LOGICAL_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_BACKLOG_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_OFFLOADED_COUNTER, dummyValue, attributes);

        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.BACKLOG_QUOTA_LIMIT_SIZE, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.BACKLOG_QUOTA_LIMIT_TIME, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.BACKLOG_EVICTION_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.BACKLOG_QUOTA_AGE, dummyValue, attributes);

        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_OUT_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.STORAGE_IN_COUNTER, dummyValue, attributes);

        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_REMOVED_COUNTED, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_SUCCEEDED_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_FAILED_COUNTER, dummyValue, attributes);
        // assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_DURATION_SECONDS, dummyValue, attributes); is double
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_BYTES_IN_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_BYTES_OUT_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_ENTRIES_COUNTER, dummyValue, attributes);
        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.COMPACTION_BYTES_COUNTER, dummyValue, attributes);

        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.TRANSACTION_COUNTER, dummyValue, attributes);

        assertOtelMetricLongSumValue(metrics, OpenTelemetryTopicStats.DELAYED_SUBSCRIPTION_COUNTER, dummyValue, attributes);
    }
}
