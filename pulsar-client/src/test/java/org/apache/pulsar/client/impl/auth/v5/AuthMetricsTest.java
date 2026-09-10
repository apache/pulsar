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
package org.apache.pulsar.client.impl.auth.v5;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.client.impl.auth.v5.AuthMetrics.AUTH_FAILURE;
import static org.apache.pulsar.client.impl.auth.v5.AuthMetrics.CREDENTIAL_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

/**
 * Verifies the two PIP-478 client-authentication metrics are emitted at the binary credential-acquisition
 * seam: {@code pulsar.client.auth.credential.duration} (histogram) on every acquisition and
 * {@code pulsar.client.auth.failure} (counter, classified terminal/transient) on failure. Mirrors the
 * in-memory OpenTelemetry reader style of {@code TlsReloadMetricsTest}.
 */
public class AuthMetricsTest {

    private static final AttributeKey<String> AUTH_METHOD = AttributeKey.stringKey("auth_method");
    private static final AttributeKey<String> ERROR = AttributeKey.stringKey("error");

    @Test
    public void recordsCredentialDurationOnSuccessAndNoFailureCounter() throws Exception {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
                .build()) {
            AuthData data = drive(sdk, new FakeBody(null)).get();
            assertThat(new String(data.getBytes(), UTF_8)).isEqualTo("cred");

            Collection<MetricData> metrics = reader.collectAllMetrics();
            HistogramPointData duration = onlyHistogramPoint(metrics, CREDENTIAL_DURATION);
            assertThat(duration.getCount()).isEqualTo(1);
            assertThat(duration.getAttributes().get(AUTH_METHOD)).isEqualTo("fake");
            // No failure counter is emitted on a successful acquisition.
            assertThat(findMetric(metrics, AUTH_FAILURE)).isNull();
        }
    }

    @Test
    public void recordsTerminalFailure() throws Exception {
        assertFailureClassifiedAs(new PulsarClientException.AuthenticationException("rejected"), "terminal");
    }

    @Test
    public void recordsTransientFailure() throws Exception {
        assertFailureClassifiedAs(new RuntimeException("identity provider unreachable"), "transient");
    }

    private void assertFailureClassifiedAs(Throwable failure, String expectedError) throws Exception {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        try (OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
                .build()) {
            CompletableFuture<AuthData> future = drive(sdk, new FakeBody(failure));
            assertThat(future).isCompletedExceptionally();

            Collection<MetricData> metrics = reader.collectAllMetrics();
            // The outcome was observed, so the duration is still recorded.
            assertThat(onlyHistogramPoint(metrics, CREDENTIAL_DURATION).getCount()).isEqualTo(1);
            LongPointData failurePoint = onlyLongPoint(metrics, AUTH_FAILURE);
            assertThat(failurePoint.getValue()).isEqualTo(1);
            assertThat(failurePoint.getAttributes().get(AUTH_METHOD)).isEqualTo("fake");
            assertThat(failurePoint.getAttributes().get(ERROR)).isEqualTo(expectedError);
        }
    }

    private static CompletableFuture<AuthData> drive(OpenTelemetrySdk sdk, FakeBody body) {
        BinaryAuthenticationExchange exchange = new BinaryAuthenticationExchange(
                body, V5AuthContexts.binaryCallContext("broker.example.com"), AuthMetrics.create(sdk));
        return exchange.getAuthDataAsync();
    }

    private static MetricData findMetric(Collection<MetricData> metrics, String name) {
        return metrics.stream().filter(m -> m.getName().equals(name)).findFirst().orElse(null);
    }

    private static HistogramPointData onlyHistogramPoint(Collection<MetricData> metrics, String name) {
        MetricData metric = findMetric(metrics, name);
        assertThat(metric).as(name).isNotNull();
        return metric.getHistogramData().getPoints().iterator().next();
    }

    private static LongPointData onlyLongPoint(Collection<MetricData> metrics, String name) {
        MetricData metric = findMetric(metrics, name);
        assertThat(metric).as(name).isNotNull();
        return metric.getLongSumData().getPoints().iterator().next();
    }

    /** A fake single-pass binary body: succeeds with a credential, or fails with the given throwable. */
    private static final class FakeBody implements Authentication, BinaryAuthDataProvider {

        private final Throwable failure;

        FakeBody(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String authMethodName() {
            return "fake";
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
            if (failure != null) {
                return CompletableFuture.failedFuture(failure);
            }
            return CompletableFuture.completedFuture(new BinaryAuthData("cred".getBytes(UTF_8)));
        }
    }
}
