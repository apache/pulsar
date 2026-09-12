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
import java.util.List;
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

    private static final List<Long> TOKEN_DURATION_BUCKET_BOUNDARIES_SECONDS = List.of(
            TimeUnit.MINUTES.toSeconds(5),
            TimeUnit.MINUTES.toSeconds(10),
            TimeUnit.HOURS.toSeconds(1),
            TimeUnit.HOURS.toSeconds(4),
            TimeUnit.DAYS.toSeconds(1),
            TimeUnit.DAYS.toSeconds(7),
            TimeUnit.DAYS.toSeconds(14),
            TimeUnit.DAYS.toSeconds(30),
            TimeUnit.DAYS.toSeconds(90),
            TimeUnit.DAYS.toSeconds(180),
            TimeUnit.DAYS.toSeconds(270),
            TimeUnit.DAYS.toSeconds(365));

    @Deprecated
    private static final Histogram expiringTokenMinutesMetrics = Histogram.build()
            .name("pulsar_expiring_token_minutes")
            .help("The remaining time of expiring token in minutes")
            .buckets(TOKEN_DURATION_BUCKET_BOUNDARIES_SECONDS.stream()
                    .map(TimeUnit.SECONDS::toMinutes)
                    .mapToDouble(Double::valueOf)
                    .toArray())
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
                .setExplicitBucketBoundariesAdvice(
                        TOKEN_DURATION_BUCKET_BOUNDARIES_SECONDS.stream().map(Double::valueOf).toList())
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
