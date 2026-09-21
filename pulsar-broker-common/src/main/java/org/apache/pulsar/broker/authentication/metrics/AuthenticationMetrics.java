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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.prometheus.client.Counter;

public class AuthenticationMetrics {
    @Deprecated
    private static final Counter authSuccessMetrics = Counter.build()
            .name("pulsar_authentication_success_total")
            .help("Pulsar authentication success")
            .labelNames("provider_name", "auth_method")
            .register();
    @Deprecated
    private static final Counter authFailuresMetrics = Counter.build()
            .name("pulsar_authentication_failures_total")
            .help("Pulsar authentication failures")
            .labelNames("provider_name", "auth_method", "reason")
            .register();

    public static final String INSTRUMENTATION_SCOPE_NAME = "org.apache.pulsar.authentication";

    /**
     * Log authenticate failure event to the authentication metrics.
     *
     * This method is deprecated due to the label "reason" is a potential infinite value.
     * @deprecated See {@link #authenticateFailure(String, String, Enum)} ()}
     *
     * @param providerName The short class name of the provider
     * @param authMethod Authentication method name.
     * @param reason Failure reason.
     */
    @Deprecated
    public static void authenticateFailure(String providerName, String authMethod, String reason) {
        authFailuresMetrics.labels(providerName, authMethod, reason).inc();
    }

    /**
     * Log authenticate failure event to the authentication metrics.
     * @param providerName The short class name of the provider
     * @param authMethod Authentication method name.
     * @param errorCode Error code.
     */
    @Deprecated
    public static void authenticateFailure(String providerName, String authMethod, Enum<?> errorCode) {
        authFailuresMetrics.labels(providerName, authMethod, errorCode.name()).inc();
    }

    public static final String AUTHENTICATION_COUNTER_METRIC_NAME = "pulsar.authentication.operation.count";
    private final LongCounter authenticationCounter;

    public static final AttributeKey<String> PROVIDER_KEY = AttributeKey.stringKey("pulsar.authentication.provider");
    public static final AttributeKey<String> AUTH_METHOD_KEY = AttributeKey.stringKey("pulsar.authentication.method");
    public static final AttributeKey<String> ERROR_CODE_KEY = AttributeKey.stringKey("pulsar.authentication.error");
    public static final AttributeKey<String> AUTH_RESULT_KEY = AttributeKey.stringKey("pulsar.authentication.result");
    public enum AuthenticationResult {
        SUCCESS,
        FAILURE;
    }

    private final String providerName;
    private final String authMethod;

    public AuthenticationMetrics(OpenTelemetry openTelemetry, String providerName, String authMethod) {
        this.providerName = providerName;
        this.authMethod = authMethod;
        var meter = openTelemetry.getMeter(INSTRUMENTATION_SCOPE_NAME);
        authenticationCounter = meter.counterBuilder(AUTHENTICATION_COUNTER_METRIC_NAME)
                .setDescription("The number of authentication operations")
                .setUnit("{operation}")
                .build();
    }

    public void recordSuccess() {
        authSuccessMetrics.labels(providerName, authMethod).inc();
        var attributes = Attributes.of(PROVIDER_KEY, providerName,
                AUTH_METHOD_KEY, authMethod,
                AUTH_RESULT_KEY, AuthenticationResult.SUCCESS.name().toLowerCase());
        authenticationCounter.add(1, attributes);
    }

    public void recordFailure(Enum<?> errorCode) {
        recordFailure(providerName, authMethod, errorCode.name());
    }

    public void recordFailure(String providerName, String authMethod, Enum<?> errorCode) {
        recordFailure(providerName, authMethod, errorCode.name());
    }

    public void recordFailure(String providerName, String authMethod, String errorCode) {
        authenticateFailure(providerName, authMethod, errorCode);
        var attributes = Attributes.of(PROVIDER_KEY, providerName,
                AUTH_METHOD_KEY, authMethod,
                AUTH_RESULT_KEY, AuthenticationResult.FAILURE.name().toLowerCase(),
                ERROR_CODE_KEY, errorCode);
        authenticationCounter.add(1, attributes);
    }
}
