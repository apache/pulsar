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
import java.util.concurrent.ExecutorService;
import lombok.Cleanup;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.NonClosingProxyHandler;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.impl.stats.BatchMetadataStoreStats;
import org.apache.pulsar.metadata.impl.stats.MetadataStoreStats;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryMetadataStoreStatsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
        setupDefaultTenantAndNamespace();

        // In testing conditions, the metadata store gets initialized before Pulsar does, so the OpenTelemetry SDK is
        // not yet initialized. Work around this issue by recreating the stats object once we have access to the SDK.
        var localMetadataStore = (MetadataStore) NonClosingProxyHandler.getDelegate(pulsar.getLocalMetadataStore());
        var currentStats = (MetadataStoreStats) FieldUtils.readField(localMetadataStore, "metadataStoreStats", true);
        var localMetadataStoreName = (String) FieldUtils.readField(currentStats, "metadataStoreName", true);

        currentStats.close();
        var newStats = new MetadataStoreStats(
                localMetadataStoreName, pulsar.getOpenTelemetry().getOpenTelemetryService().getOpenTelemetry());
        FieldUtils.writeField(localMetadataStore, "metadataStoreStats", newStats, true);

        var currentBatchedStats = (BatchMetadataStoreStats) FieldUtils.readField(localMetadataStore, "batchMetadataStoreStats", true);
        currentBatchedStats.close();
        var currentExecutor = (ExecutorService) FieldUtils.readField(currentBatchedStats, "executor", true);
        var newBatchedStats = new BatchMetadataStoreStats(
                localMetadataStoreName, currentExecutor, pulsar.getOpenTelemetry().getOpenTelemetryService().getOpenTelemetry());
        FieldUtils.writeField(localMetadataStore, "batchMetadataStoreStats", newBatchedStats, true);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        pulsarTestContextBuilder.enableOpenTelemetry(true);
    }

    @Test
    public void testMetadataStoreStats() throws Exception {
        var topicName = BrokerTestUtil.newUniqueName("persistent://public/default/test-metadata-store-stats");

        @Cleanup
        var producer = pulsarClient.newProducer().topic(topicName).create();

        producer.newMessage().value("test".getBytes()).send();

        var attributes = Attributes.of(MetadataStoreStats.METADATA_STORE_NAME, "metadata-store");

        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        assertMetricLongSumValue(metrics, MetadataStoreStats.METADATA_STORE_PUT_BYTES_COUNTER_METRIC_NAME,
                attributes, value -> assertThat(value).isPositive());
        assertMetricLongSumValue(metrics, BatchMetadataStoreStats.EXECUTOR_QUEUE_SIZE_METRIC_NAME, attributes,
                value -> assertThat(value).isPositive());
    }
}
