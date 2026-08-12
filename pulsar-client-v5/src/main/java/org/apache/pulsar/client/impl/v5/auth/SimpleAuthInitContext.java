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
package org.apache.pulsar.client.impl.v5.auth;

import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.http.PulsarHttpClientFactory;

/**
 * Simple immutable {@link AuthenticationInitContext} implementation used by the v5/v4 bridges to
 * initialize a v5 {@link org.apache.pulsar.client.api.v5.auth.Authentication} plugin.
 */
class SimpleAuthInitContext implements AuthenticationInitContext {

    private final PulsarHttpClientFactory httpClientFactory;
    private final ScheduledExecutorService scheduler;
    private final Executor blockingExecutor;
    private final Clock clock;
    private final OpenTelemetry openTelemetry;
    private final String clientInstanceId;

    SimpleAuthInitContext(PulsarHttpClientFactory httpClientFactory, ScheduledExecutorService scheduler,
            Executor blockingExecutor, Clock clock, OpenTelemetry openTelemetry, String clientInstanceId) {
        this.httpClientFactory = httpClientFactory;
        this.scheduler = scheduler;
        this.blockingExecutor = blockingExecutor;
        this.clock = clock;
        this.openTelemetry = openTelemetry;
        this.clientInstanceId = clientInstanceId;
    }

    @Override
    public PulsarHttpClientFactory httpClientFactory() {
        return httpClientFactory;
    }

    @Override
    public ScheduledExecutorService scheduler() {
        return scheduler;
    }

    @Override
    public Executor blockingExecutor() {
        return blockingExecutor;
    }

    @Override
    public Clock clock() {
        return clock;
    }

    @Override
    public OpenTelemetry openTelemetry() {
        return openTelemetry;
    }

    @Override
    public String clientInstanceId() {
        return clientInstanceId;
    }
}
