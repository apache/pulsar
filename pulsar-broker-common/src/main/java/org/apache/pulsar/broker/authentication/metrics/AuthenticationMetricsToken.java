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
package org.apache.pulsar.broker.authentication.metrics;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.stats.MetricsUtil;

public class AuthenticationMetricsToken extends AuthenticationMetrics {

    @Deprecated
    private static final Counter expiredTokenMetrics = Counter.build()
            .name("pulsar_expired_token_total")
            .help("Pulsar expired token")
            .register();
    public static final String EXPIRED_TOKEN_COUNTER_METRIC_NAME = "pulsar.authentication.token.expired.count";
    private LongCounter expiredTokensCounter;

    @Deprecated
    private static final Histogram expiringTokenMinutesMetrics = Histogram.build()
            .name("pulsar_expiring_token_minutes")
            .help("The remaining time of expiring token in minutes")
            .buckets(5, 10, 60, 240)
            .register();
    public static final String EXPIRING_TOKEN_HISTOGRAM_METRIC_NAME = "pulsar.authentication.token.expiry.duration";
    private DoubleHistogram expiringTokenSeconds;

    public AuthenticationMetricsToken(OpenTelemetry openTelemetry, String providerName,
                                      String authMethod) {
        super(openTelemetry, providerName, authMethod);

        var meter = openTelemetry.getMeter(AuthenticationMetrics.INSTRUMENTATION_SCOPE_NAME);
        expiredTokensCounter = meter.counterBuilder(EXPIRED_TOKEN_COUNTER_METRIC_NAME)
                .setDescription("The total number of expired tokens")
                .setUnit("{token}")
                .build();
        expiringTokenSeconds = meter.histogramBuilder(EXPIRING_TOKEN_HISTOGRAM_METRIC_NAME)
                .setDescription("The remaining time of expiring token in seconds")
                .setUnit("s")
                .build();
    }

    public void recordTokenDuration(Long durationMs) {
        if (durationMs == null) {
            // Special case signals a token without expiry. OpenTelemetry supports reporting infinite values.
            expiringTokenSeconds.record(Double.POSITIVE_INFINITY);
        } else if (durationMs > 0) {
            expiringTokenMinutesMetrics.observe(durationMs / 60_000.0d);
            expiringTokenSeconds.record(MetricsUtil.convertToSeconds(durationMs, TimeUnit.MILLISECONDS));
        } else {
            // Duration can be negative if token expires at processing time. OpenTelemetry does not support negative
            // values, so record token expiry instead.
            recordTokenExpired();
        }
    }

    public void recordTokenExpired() {
        expiredTokenMetrics.inc();
        expiredTokensCounter.add(1);
    }

    @VisibleForTesting
    @Deprecated
    public static void reset() {
        expiredTokenMetrics.clear();
        expiringTokenMinutesMetrics.clear();
    }
}
