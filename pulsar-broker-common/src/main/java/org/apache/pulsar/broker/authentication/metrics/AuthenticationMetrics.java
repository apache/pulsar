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

import io.prometheus.client.Counter;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.authentication.BaseAuthenticationException.PulsarAuthenticationException;
import org.apache.pulsar.broker.authentication.BaseAuthenticationException.PulsarAuthenticationException.ErrorCode;

public class AuthenticationMetrics {
    private static final Counter authSuccessMetrics = Counter.build()
            .name("pulsar_authentication_success_total")
            .help("Pulsar authentication success")
            .labelNames("provider_name", "auth_method")
            .register();
    private static final Counter authFailuresMetrics = Counter.build()
            .name("pulsar_authentication_failures_total")
            .help("Pulsar authentication failures")
            .labelNames("provider_name", "auth_method", "reason")
            .register();

    /**
     * Log authenticate success event to the authentication metrics.
     * @param providerName The short class name of the provider
     * @param authMethod Authentication method name
     */
    public static void authenticateSuccess(String providerName, String authMethod) {
        authSuccessMetrics.labels(providerName, authMethod).inc();
    }

    /**
     * Log authenticate failure event to the authentication metrics.
     *
     * This method is deprecated due to the label "reason" is a potential infinite value.
     * @deprecated See {@link #authenticateFailure(String, String, ErrorCode)} ()}
     * or {@link #authenticateFailure(String, String, AuthenticationException)}
     *
     * @param providerName The short class name of the provider
     * @param authMethod Authentication method name.
     * @param reason Failure reason.
     */
    @Deprecated
    public static void authenticateFailure(String providerName, String authMethod, String reason) {
        authFailuresMetrics.labels(providerName, authMethod, reason).inc();
    }

    public static void authenticateFailure(String providerName, String authMethod,
                                           AuthenticationException exception) {
        ErrorCode errorCode = exception instanceof PulsarAuthenticationException
                ? ((PulsarAuthenticationException) exception).getErrorCode() : ErrorCode.UNKNOWN;
        authenticateFailure(providerName, authMethod, errorCode);
    }

    /**
     * Log authenticate failure event to the authentication metrics.
     * @param providerName The short class name of the provider
     * @param authMethod Authentication method name.
     * @param errorCode Error code.
     */
    public static void authenticateFailure(String providerName, String authMethod, ErrorCode errorCode) {
        authFailuresMetrics.labels(providerName, authMethod, errorCode.name()).inc();
    }


}
