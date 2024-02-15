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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.broker.testcontext.SpyConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
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

    @Test(timeOut = 30_000, invocationCount = 100, skipFailedInvocations = true)
    public void testPendingLookupRequestsCounter() throws Exception {
        var topicName = TopicName.get(BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testTopic"));
        var namespaceService = pulsar.getNamespaceService();
        var metricReader = pulsarTestContext.getOpenTelemetryMetricReader();

        // Wait until no requests are ongoing.
        Awaitility.await().untilAsserted(() ->
            assertThat(metricReader.collectAllMetrics())
                .anySatisfy(metric -> assertThat(metric)
                    .hasName("pulsar.broker.lookup.pending.request")
                    .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(0)))));

        var cdl = new CountDownLatch(1);

        doAnswer(invocation -> {
            Awaitility.await().untilAsserted(() ->
                assertThat(metricReader.collectAllMetrics())
                    .anySatisfy(metric -> assertThat(metric)
                        .hasName("pulsar.broker.lookup.pending.request")
                        .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(1)))));
            cdl.countDown();
            return invocation.callRealMethod();
        }).doCallRealMethod().when(namespaceService).getBundleAsync(topicName);

        doAnswer(invocation -> {
            Awaitility.await().untilAsserted(() ->
                assertThat(metricReader.collectAllMetrics())
                    .anySatisfy(metric -> assertThat(metric)
                        .hasName("pulsar.broker.topic.load.pending.request")
                        .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(1)))));
            cdl.countDown();
            return invocation.callRealMethod();
        }).doCallRealMethod().when(namespaceService).isServiceUnitActiveAsync(topicName);

        @Cleanup
        var consumer = pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName("mysub").subscribe();

        cdl.await();
    }

    @Test(timeOut = 30_000, invocationCount = 100, skipFailedInvocations = true)
    public void testPendingLoadTopicRequestsCounter() throws Exception {
        var topicName = TopicName.get(BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testTopic"));
        var namespaceService = pulsar.getNamespaceService();
        var metricReader = pulsarTestContext.getOpenTelemetryMetricReader();

        // Wait until no requests are ongoing.
        Awaitility.await().untilAsserted(() ->
            assertThat(metricReader.collectAllMetrics())
                .anySatisfy(metric -> assertThat(metric)
                    .hasName("pulsar.broker.topic.load.pending.request")
                    .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(0)))));

        var cdl = new CountDownLatch(1);

        doAnswer(invocation -> {
            Awaitility.await().untilAsserted(() ->
                assertThat(metricReader.collectAllMetrics())
                    .anySatisfy(metric -> assertThat(metric)
                        .hasName("pulsar.broker.topic.load.pending.request")
                        .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(1)))));
            cdl.countDown();
            return invocation.callRealMethod();
        }).doCallRealMethod().when(namespaceService).isServiceUnitActiveAsync(topicName);

        @Cleanup
        var consumer = pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName("mysub").subscribe();

        cdl.await();
    }

    @Test(timeOut = 30_000, invocationCount = 100, skipFailedInvocations = true)
    public void testLookupAnswersCounter() throws Exception {
        var topicName = TopicName.get(BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testTopic"));
        var metricReader = pulsarTestContext.getOpenTelemetryMetricReader();

        assertThat(metricReader.collectAllMetrics())
            .noneSatisfy(metric -> assertThat(metric).hasName("pulsar.broker.lookup.answer"));

        @Cleanup
        var consumer = pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName("mysub").subscribe();

        Awaitility.await().untilAsserted(() ->
            assertThat(metricReader.collectAllMetrics())
                .anySatisfy(metric -> assertThat(metric)
                    .hasName("pulsar.broker.lookup.answer")
                    .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(2)))));

        // Also verify latency numbers are being recorded.
        assertThat(metricReader.collectAllMetrics()).anySatisfy(metric -> assertThat(metric)
            .hasName("pulsar.broker.lookup.latency")
            .hasHistogramSatisfying(histogram -> histogram.hasPointsSatisfying(point -> point.hasCount(2))));
    }

    @Test(timeOut = 30_000, invocationCount = 100, skipFailedInvocations = true)
    public void testLookupFailureCounter() throws Exception {
        var topicName = TopicName.get(BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testTopic"));
        var namespaceService = pulsar.getNamespaceService();
        var metricReader = pulsarTestContext.getOpenTelemetryMetricReader();

        assertThat(metricReader.collectAllMetrics())
            .noneSatisfy(metric -> assertThat(metric).hasName("pulsar.broker.lookup.failure"));

        doAnswer(__ -> CompletableFuture.failedFuture(new Exception())).doCallRealMethod()
                .when(namespaceService).getBundleAsync(topicName);

        @Cleanup
        var consumer = pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName("mysub").subscribe();

        Awaitility.await().untilAsserted(() ->
            assertThat(metricReader.collectAllMetrics())
                .anySatisfy(metric -> assertThat(metric)
                    .hasName("pulsar.broker.lookup.failure")
                    .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(1)))));
    }

    @Test(timeOut = 30_000, invocationCount = 100, skipFailedInvocations = true)
    public void testLookupRedirectCounter() throws Exception {
        var topicName = TopicName.get(BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testTopic"));
        var namespaceService = pulsar.getNamespaceService();
        var metricReader = pulsarTestContext.getOpenTelemetryMetricReader();

        assertThat(metricReader.collectAllMetrics())
            .noneSatisfy(metric -> assertThat(metric).hasName("pulsar.broker.lookup.redirect"));

        var lookupResult = Mockito.mock(LookupResult.class);
        doReturn(true).when(lookupResult).isRedirect();

        var future = CompletableFuture.completedFuture(Optional.of(lookupResult));
        doReturn(future).doCallRealMethod().when(namespaceService).findBrokerServiceUrl(Mockito.any(), Mockito.any());

        @Cleanup
        var consumer = pulsarClient.newConsumer().topic(topicName.toString()).subscriptionName("mysub").subscribe();

        Awaitility.await().untilAsserted(() ->
            assertThat(metricReader.collectAllMetrics())
                .anySatisfy(metric -> assertThat(metric)
                    .hasName("pulsar.broker.lookup.redirect")
                    .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(1)))));
    }
}
