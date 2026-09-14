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
package org.apache.pulsar.client.api.v5.auth;

import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.http.PulsarHttpClientFactory;

/**
 * Runtime services handed to {@link Authentication#initializeAsync} once when the client is built
 * (PIP-478).
 *
 * <p>The framework owns and closes these shared services (HTTP clients, scheduler, blocking executor);
 * the implementation may retain references for the lifetime of the client and releases its own
 * resources in {@link Authentication#close()}.
 */
public interface AuthenticationInitContext {

    /**
     * Factory for {@link org.apache.pulsar.http.PulsarHttpClient} instances. The
     * framework manages lifecycle (event loop, timer, DNS cache, TLS material refresh integration);
     * plugins describe what they need via a
     * {@link org.apache.pulsar.http.PulsarHttpClientConfig} and receive a configured
     * instance owned by the framework. Multiple instances with different TLS / timeouts may be
     * obtained for different uses (e.g. OAuth2 mTLS to the IdP vs HTTP topic lookup).
     *
     * <p>Plugins MUST NOT construct private HTTP clients directly — doing so defeats the framework's
     * shared event-loop / DNS / refresh integration.
     *
     * @return the framework's HTTP client factory
     */
    PulsarHttpClientFactory httpClientFactory();

    /**
     * Scheduler for delayed / periodic authentication work (e.g. proactive credential renewal). Never
     * the Netty event loop. Reserved for scheduled tasks — potentially-blocking work belongs on
     * {@link #blockingExecutor()}.
     *
     * @return the framework-owned scheduler
     */
    ScheduledExecutorService scheduler();

    /**
     * Dedicated executor for off-loading potentially-blocking authentication work, kept separate from
     * {@link #scheduler()} so that blocking calls cannot starve the scheduler's threads or delay
     * scheduled tasks. The {@code LegacyV4AuthenticationAdapter} offloads every synchronous v4 plugin
     * call here. Never the Netty event loop. Framework-owned and shared per {@code PulsarClient}.
     *
     * @return the framework-owned blocking executor
     */
    Executor blockingExecutor();

    /**
     * @return the clock used by implementations that schedule against wall-clock time
     */
    Clock clock();

    /**
     * @return the telemetry root; the framework defaults to {@link OpenTelemetry#noop()} if unset
     */
    OpenTelemetry openTelemetry();

    /**
     * @return a stable id of the owning {@code PulsarClient}, for logging correlation
     */
    String clientInstanceId();
}
