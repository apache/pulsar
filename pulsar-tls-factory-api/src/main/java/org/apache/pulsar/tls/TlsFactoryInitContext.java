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
package org.apache.pulsar.tls;

import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The runtime services handed to a {@link PulsarTlsFactory#initialize(TlsFactoryInitContext)} call
 * (PIP-478).
 *
 * <p>The context is constructed by whichever component owns the factory — the v5 client builder on the
 * client side; the broker / proxy / websocket / functions-worker service on the server side — and
 * {@code initialize(...)} completes before the first {@code createInstance} call. The framework owns
 * and closes these shared services; the factory may retain references for its lifetime.
 */
public interface TlsFactoryInitContext {

    /**
     * Factory-specific parameters from the owning component's configuration (the
     * {@code tlsFactoryConfig} key on the server side; builder-supplied on the client).
     *
     * @return the factory parameters (possibly empty)
     */
    Map<String, String> params();

    /**
     * @return a framework-owned scheduler for file-watch polling and rotation work; never a consumer
     *         event loop
     */
    ScheduledExecutorService scheduler();

    /**
     * @return an executor for potentially-blocking material loading; never a consumer event loop
     */
    Executor blockingExecutor();

    /**
     * @return the clock a factory should read the current time from — for example to age a cached
     *         context or stamp a rotation — injectable so tests can drive time deterministically
     *         instead of sleeping; never {@code null}
     */
    Clock clock();

    /**
     * @return the telemetry root; the framework defaults to {@link OpenTelemetry#noop()} if unset
     */
    OpenTelemetry openTelemetry();
}
