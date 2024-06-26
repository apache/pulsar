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
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
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
}
