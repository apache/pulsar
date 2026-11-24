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
package org.apache.pulsar.client.api.v5.internal;

import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.http.PulsarHttpClientFactory;

/**
 * The framework-owned runtime services a {@code PulsarClient} late-binds into its authentication driver
 * once the client instance exists (PIP-478).
 *
 * <p>An {@code Authentication} plugin is configured on the client builder <em>before</em> the client is
 * constructed (via {@code conf.setAuthentication(...)}), so these services cannot be supplied at plugin
 * construction time. The client therefore binds them right after it has created its own executors and
 * HTTP client factory, but <em>before</em> it starts the plugin — see
 * {@link ClientAuthenticationServicesAware}. The services mirror the fields the plugin ultimately sees
 * through its {@link org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext}.
 *
 * <p>The framework owns and closes these shared services; a driver may retain the reference for the
 * lifetime of the client.
 *
 * <p>The {@code .internal.} subpackage signals "stable internal" — application code should not consume
 * this type; it is passed between the framework's own client and its authentication bridges.
 */
public interface ClientAuthenticationServices {

    /**
     * @return the framework's AsyncHttpClient-backed HTTP client factory, sharing the owning client's
     *         event loop / timer / DNS resources; never {@code null}
     */
    PulsarHttpClientFactory httpClientFactory();

    /**
     * @return the framework-owned scheduler for delayed / periodic authentication work; never the Netty
     *         event loop
     */
    ScheduledExecutorService scheduler();

    /**
     * @return the framework-owned bounded executor for off-loading potentially-blocking authentication
     *         work (credential I/O, legacy v4 plugin calls); never the Netty event loop
     */
    Executor blockingExecutor();

    /**
     * @return the clock implementations schedule against
     */
    Clock clock();

    /**
     * @return the telemetry root ({@link OpenTelemetry#noop()} when unset)
     */
    OpenTelemetry openTelemetry();

    /**
     * @return a stable id of the owning {@code PulsarClient}, for logging correlation
     */
    String clientInstanceId();
}
