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

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import io.jsonwebtoken.SignatureAlgorithm;
import io.opentelemetry.api.common.Attributes;
import java.time.Duration;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetricsToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryAuthenticationStatsTest extends BrokerTestBase {

    private static final Duration AUTHENTICATION_TIMEOUT = Duration.ofSeconds(1);

    private SecretKey secretKey;
    private AuthenticationProvider provider;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();

        secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        provider = new AuthenticationProviderToken();
        registerCloseable(provider);

        var properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        var conf = new ServiceConfiguration();
        conf.setProperties(properties);

        var authenticationProviderContext = AuthenticationProvider.Context.builder()
                .config(conf)
                .openTelemetry(pulsar.getOpenTelemetry().getOpenTelemetry())
                .build();
        provider.initialize(authenticationProviderContext);
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
    public void testAuthenticationSuccess() {
        // Pulsar protocol auth
        assertThat(provider.authenticateAsync(new TestAuthenticationDataSource(Optional.empty())))
                .succeedsWithin(AUTHENTICATION_TIMEOUT);
        assertMetricLongSumValue(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics(),
                AuthenticationMetrics.AUTHENTICATION_COUNTER_METRIC_NAME,
                Attributes.of(AuthenticationMetrics.PROVIDER_KEY, "AuthenticationProviderToken",
                        AuthenticationMetrics.AUTH_RESULT_KEY, "success",
                        AuthenticationMetrics.AUTH_METHOD_KEY, "token"),
                1);
    }

    @Test
    public void testTokenDurationHistogram() {
        // Token with expiry 15 seconds into the future
        var expiryTime = Optional.of(new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(15)));
        assertThat(provider.authenticateAsync(new TestAuthenticationDataSource(expiryTime)))
                .succeedsWithin(AUTHENTICATION_TIMEOUT);
        // Token without expiry
        assertThat(provider.authenticateAsync(new TestAuthenticationDataSource(Optional.empty())))
                .succeedsWithin(AUTHENTICATION_TIMEOUT);
        assertThat(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics())
                .anySatisfy(metric -> assertThat(metric)
                        .hasName(AuthenticationMetricsToken.EXPIRING_TOKEN_HISTOGRAM_METRIC_NAME)
                        .hasHistogramSatisfying(histogram -> histogram.hasPointsSatisfying(
                                histogramPoint -> histogramPoint.hasCount(2).hasMax(Double.POSITIVE_INFINITY))));
    }

    @Test
    public void testAuthenticationFailure() {
        // Authentication should fail if credentials not passed.
        assertThat(provider.authenticateAsync(new AuthenticationDataSource() { }))
                .failsWithin(AUTHENTICATION_TIMEOUT)
                .withThrowableThat()
                .withRootCauseInstanceOf(AuthenticationException.class)
                .withMessageContaining("No token credentials passed");
        assertMetricLongSumValue(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics(),
                AuthenticationMetrics.AUTHENTICATION_COUNTER_METRIC_NAME,
                Attributes.of(AuthenticationMetrics.PROVIDER_KEY, "AuthenticationProviderToken",
                        AuthenticationMetrics.AUTH_RESULT_KEY, "failure",
                        AuthenticationMetrics.AUTH_METHOD_KEY, "token",
                        AuthenticationMetrics.ERROR_CODE_KEY, "INVALID_AUTH_DATA"),
                1);
    }

    @Test
    public void testTokenExpired() {
        var expiredDate = Optional.of(new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)));
        assertThat(provider.authenticateAsync(new TestAuthenticationDataSource(expiredDate)))
                .failsWithin(AUTHENTICATION_TIMEOUT)
                .withThrowableThat()
                .withRootCauseInstanceOf(AuthenticationException.class)
                .withMessageContaining("JWT expired");
        assertMetricLongSumValue(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics(),
                AuthenticationMetricsToken.EXPIRED_TOKEN_COUNTER_METRIC_NAME, Attributes.empty(), 1);
    }

    private class TestAuthenticationDataSource implements AuthenticationDataSource {
        private final String token;

        public TestAuthenticationDataSource(Optional<Date> expiryTime) {
            token = AuthTokenUtils.createToken(secretKey, "subject", expiryTime);
        }

        @Override
        public boolean hasDataFromCommand() {
            return true;
        }

        @Override
        public String getCommandData() {
            return token;
        }
    }
}
