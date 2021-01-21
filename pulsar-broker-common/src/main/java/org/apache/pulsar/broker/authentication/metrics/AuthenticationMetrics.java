/**
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

public class AuthenticationMetrics {
    private static final Counter authSuccessMetrics = Counter.build()
            .name("pulsar_authentication_success_count")
            .help("Pulsar authentication success")
            .labelNames("auth_method")
            .register();
    private static final Counter authFailuresMetrics = Counter.build()
            .name("pulsar_authentication_failures_count")
            .help("Pulsar authentication failures")
            .labelNames("auth_method", "reason")
            .register();

    /**
     * Log authenticate success event to the authentication metrics
     * @param authMethod Authentication method name
     */
    public static void authenticateSuccess(String authMethod) {
        authSuccessMetrics.labels(authMethod).inc();
    }

    /**
     * Log authenticate failure event to the authentication metrics.
     * @param authMethod Authentication method name.
     * @param reason Failure reason.
     */
    public static void authenticateFailure(String authMethod, String reason) {
        authFailuresMetrics.labels(authMethod, reason).inc();
    }
}
