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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ImmutablePositionImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.ConnectionCreateStatus;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryBrokerOperabilityStatsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        pulsarTestContextBuilder.enableOpenTelemetry(true);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testBrokerConnection() throws Exception {
        var topicName = BrokerTestUtil.newUniqueName("persistent://my-namespace/use/my-ns/testBrokerConnection");

        @Cleanup
        var producer = pulsarClient.newProducer().topic(topicName).create();

        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_COUNTER_METRIC_NAME,
                OpenTelemetryAttributes.ConnectionStatus.OPEN.attributes, 1);
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_COUNTER_METRIC_NAME,
                OpenTelemetryAttributes.ConnectionStatus.CLOSE.attributes, 0);
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_COUNTER_METRIC_NAME,
                OpenTelemetryAttributes.ConnectionStatus.ACTIVE.attributes, 1);

        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_CREATE_COUNTER_METRIC_NAME,
                ConnectionCreateStatus.SUCCESS.attributes, 1);
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_CREATE_COUNTER_METRIC_NAME,
                ConnectionCreateStatus.FAILURE.attributes, 0);

        pulsarClient.close();

        metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_COUNTER_METRIC_NAME,
                OpenTelemetryAttributes.ConnectionStatus.CLOSE.attributes, 1);

        pulsar.getConfiguration().setAuthenticationEnabled(true);

        replacePulsarClient(PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .operationTimeout(1, TimeUnit.MILLISECONDS));
        assertThatThrownBy(() -> pulsarClient.newProducer().topic(topicName).create())
                .isInstanceOf(PulsarClientException.AuthenticationException.class);
        pulsarClient.close();

        metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_COUNTER_METRIC_NAME,
                OpenTelemetryAttributes.ConnectionStatus.OPEN.attributes, 2);
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_COUNTER_METRIC_NAME,
                OpenTelemetryAttributes.ConnectionStatus.CLOSE.attributes, 2);
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_COUNTER_METRIC_NAME,
                OpenTelemetryAttributes.ConnectionStatus.ACTIVE.attributes, 0);

        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_CREATE_COUNTER_METRIC_NAME,
                ConnectionCreateStatus.SUCCESS.attributes, 1);
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.CONNECTION_CREATE_COUNTER_METRIC_NAME,
                ConnectionCreateStatus.FAILURE.attributes, 1);
    }

    @Test
    public void testNonRecoverableDataMetrics() throws Exception {
        BrokerOperabilityMetrics brokerOperabilityMetrics = pulsar.getBrokerService()
                .getPulsarStats().getBrokerOperabilityMetrics();

        // Test initial state - should be 0
        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.NON_RECOVERABLE_LEDGERS_SKIPPED_COUNTER_METRIC_NAME,
                io.opentelemetry.api.common.Attributes.empty(), 0);
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.NON_RECOVERABLE_ENTRIES_SKIPPED_COUNTER_METRIC_NAME,
                io.opentelemetry.api.common.Attributes.empty(), 0);

        // Test recording ledger skip metrics
        brokerOperabilityMetrics.recordNonRecoverableLedgerSkipped();
        brokerOperabilityMetrics.recordNonRecoverableLedgerSkipped();

        // Test recording entry skip metrics
        brokerOperabilityMetrics.recordNonRecoverableEntriesSkipped();
        brokerOperabilityMetrics.recordNonRecoverableEntriesSkipped();
        brokerOperabilityMetrics.recordNonRecoverableEntriesSkipped();

        // Verify the metrics have been updated
        metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.NON_RECOVERABLE_LEDGERS_SKIPPED_COUNTER_METRIC_NAME,
                io.opentelemetry.api.common.Attributes.empty(), 2);
        assertMetricLongSumValue(metrics, BrokerOperabilityMetrics.NON_RECOVERABLE_ENTRIES_SKIPPED_COUNTER_METRIC_NAME,
                io.opentelemetry.api.common.Attributes.empty(), 3);
    }

    @Test
    public void testEndToEndManagedLedgerCallbackIntegration() throws Exception {
        BrokerOperabilityMetrics brokerOperabilityMetrics = pulsar.getBrokerService()
                .getPulsarStats().getBrokerOperabilityMetrics();

        // Get initial metric values
        Collection<MetricData> initialMetrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        long initialLedgersSkipped = getMetricValue(initialMetrics,
                BrokerOperabilityMetrics.NON_RECOVERABLE_LEDGERS_SKIPPED_COUNTER_METRIC_NAME);
        long initialEntriesSkipped = getMetricValue(initialMetrics,
                BrokerOperabilityMetrics.NON_RECOVERABLE_ENTRIES_SKIPPED_COUNTER_METRIC_NAME);

        // Test end-to-end callback integration by creating a real topic and subscription
        // This ensures we're testing the actual callback that BrokerService sets up during topic creation
        String topicName = BrokerTestUtil.newUniqueName("persistent://my-namespace/use/my-ns/testEndToEndCallback");

        // Create a consumer, which will automatically create the topic and subscription
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("test-subscription")
                .subscribe();
        @Cleanup
        Producer<byte[]> producer =  pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .create();

        List<MessageIdAdv> messageIds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messageIds.add((MessageIdAdv) producer.newMessage().value(("message-" + i).getBytes()).send());
        }

        // Get the topic instance from the broker service
        Topic topic = pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        PersistentTopic persistentTopic = (PersistentTopic) topic;

        // Get the managed ledger and cursor from the topic - these have the callback configured by BrokerService
        ManagedLedger managedLedger = persistentTopic.getManagedLedger();
        PersistentSubscription subscription = persistentTopic.getSubscription("test-subscription");
        ManagedCursorImpl cursor = (ManagedCursorImpl) subscription.getCursor();

        long ledgerId = messageIds.get(0).getLedgerId();
        long firstEntryId = messageIds.get(0).getEntryId();
        long lastEntryId = messageIds.get(messageIds.size() - 1).getEntryId();

        // Test skipNonRecoverableLedger - this should trigger the callback set up by BrokerService
        managedLedger.skipNonRecoverableLedger(ledgerId);
        cursor.skipNonRecoverableEntries(new ImmutablePositionImpl(ledgerId, firstEntryId),
                new ImmutablePositionImpl(ledgerId, lastEntryId + 1));

        // Verify the metrics were updated through the callback chain
        Collection<MetricData> finalMetrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();

        // Verify that the ledger skip metrics increased by 1 (we called skipNonRecoverableLedger once)
        assertMetricLongSumValue(finalMetrics,
                BrokerOperabilityMetrics.NON_RECOVERABLE_LEDGERS_SKIPPED_COUNTER_METRIC_NAME,
                io.opentelemetry.api.common.Attributes.empty(), initialLedgersSkipped + 1);

        // Verify that the entries skip metrics increased by 10 (10 entries skipped)
        assertMetricLongSumValue(finalMetrics,
                BrokerOperabilityMetrics.NON_RECOVERABLE_ENTRIES_SKIPPED_COUNTER_METRIC_NAME,
                io.opentelemetry.api.common.Attributes.empty(), initialEntriesSkipped + 10);
    }

    private long getMetricValue(java.util.Collection<io.opentelemetry.sdk.metrics.data.MetricData> metrics,
                                String metricName) {
        return metrics.stream()
                .filter(m -> m.getName().equals(metricName))
                .findFirst()
                .map(metricData -> metricData.getLongSumData().getPoints().stream()
                        .findFirst()
                        .map(point -> point.getValue())
                        .orElse(0L))
                .orElse(0L);
    }
}
