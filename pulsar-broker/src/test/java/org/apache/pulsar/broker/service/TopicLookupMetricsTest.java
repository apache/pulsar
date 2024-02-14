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
package org.apache.pulsar.broker.service;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import lombok.Cleanup;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.broker.testcontext.SpyConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicLookupMetricsTest extends BrokerTestBase {

    @BeforeMethod
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
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        pulsarTestContextBuilder.enableOpenTelemetry(true);
        pulsarTestContextBuilder.spyConfigCustomizer(
                builder -> builder.namespaceService(SpyConfig.SpyType.SPY_ALSO_INVOCATIONS));
    }

    @Test(timeOut = 30_000)
    public void testPendingLookupRequestsCounter() throws Exception {
        var topicName = TopicName.get("persistent://prop/ns-abc/newTopic");
        var namespaceService = pulsar.getNamespaceService();
        var metricReader = pulsarTestContext.getOpenTelemetryMetricReader();

        // Wait until no requests are ongoing.
        Awaitility.await().untilAsserted(() ->
            assertThat(metricReader.collectAllMetrics())
                .anySatisfy(metric -> assertThat(metric)
                    .hasName("pulsar.broker.lookup.pending.request")
                    .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(0)))));

        doAnswer(invocation -> {
            Awaitility.await().untilAsserted(() ->
                assertThat(metricReader.collectAllMetrics())
                    .anySatisfy(metric -> assertThat(metric)
                        .hasName("pulsar.broker.lookup.pending.request")
                        .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(1)))));
            return invocation.callRealMethod();
        }).doCallRealMethod().when(namespaceService).getBundleAsync(topicName);

        @Cleanup
        var consumer = pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName("mysub").subscribe();

        verify(namespaceService, atLeast(1)).getBundleAsync(topicName);
    }
}
