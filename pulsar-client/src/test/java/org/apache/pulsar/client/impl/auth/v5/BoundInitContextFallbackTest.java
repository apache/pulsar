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

import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.testng.annotations.Test;

/**
 * PIP-478: {@link AuthenticationInitContext#scheduler()} and
 * {@link AuthenticationInitContext#blockingExecutor()} are documented as never {@code null}, and the
 * no-services context honours that. A component may still bind <em>partial</em> services — the admin binds
 * an HTTP client factory and a blocking executor but no scheduler, because nothing on its path schedules
 * periodic authentication work — and without a fallback on the bound path, binding partial services would
 * be worse for a plugin than binding none at all.
 */
public class BoundInitContextFallbackTest {

    @Test
    public void aBoundContextWithNoSchedulerStillSuppliesOne() {
        AuthenticationInitContext ctx = V5AuthContexts.initContext(
                new DefaultClientAuthenticationServices(null, null, null, Clock.systemUTC(),
                        OpenTelemetry.noop(), "test-client"),
                "unused");

        assertThat(ctx.scheduler()).as("a plugin scheduling a credential refresh must not NPE").isNotNull();
        assertThat(ctx.blockingExecutor()).as("the SPI tells plugins to off-load here").isNotNull();
    }

    @Test
    public void boundExecutorsAreUsedWhenSupplied() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Executor blocking = Runnable::run;
        try {
            AuthenticationInitContext ctx = V5AuthContexts.initContext(
                    new DefaultClientAuthenticationServices(null, scheduler, blocking, Clock.systemUTC(),
                            OpenTelemetry.noop(), "test-client"),
                    "unused");

            // The fallback must not shadow what a component did bind.
            assertThat(ctx.scheduler()).isSameAs(scheduler);
            assertThat(ctx.blockingExecutor()).isSameAs(blocking);
        } finally {
            scheduler.shutdownNow();
        }
    }
}
