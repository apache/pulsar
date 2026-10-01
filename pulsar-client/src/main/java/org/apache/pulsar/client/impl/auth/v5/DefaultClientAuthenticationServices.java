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

import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.http.PulsarHttpClientFactory;

/**
 * Immutable {@link ClientAuthenticationServices} the {@code PulsarClientImpl} late-binds into its
 * authentication driver (PIP-478).
 *
 * @param httpClientFactory the framework HTTP client factory
 * @param scheduler         the framework scheduler
 * @param blockingExecutor  the framework bounded blocking executor
 * @param clock             the clock implementations schedule against
 * @param openTelemetry     the telemetry root
 * @param clientInstanceId  a stable id for the owning client
 */
public record DefaultClientAuthenticationServices(
        PulsarHttpClientFactory httpClientFactory,
        ScheduledExecutorService scheduler,
        Executor blockingExecutor,
        Clock clock,
        OpenTelemetry openTelemetry,
        String clientInstanceId) implements ClientAuthenticationServices {
}
