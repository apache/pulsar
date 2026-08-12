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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.testng.annotations.Test;

/**
 * PIP-478: a failed {@code initializeAsync} must not be cached. The SPI documents that the framework
 * retries on the next use, so a transient failure — an IdP that is not reachable yet at client start —
 * recovers on reconnect instead of poisoning the plugin for the client's lifetime.
 *
 * <p>The bug this pins is specific to an <em>already-failed</em> future: {@code whenComplete} then runs its
 * callback synchronously, so a driver that cleared the memo inside that callback before assigning the field
 * had the assignment put the failure straight back. Only a body that fails <em>synchronously</em> (returns
 * {@code CompletableFuture.failedFuture(...)}) reproduces it; one that fails later does not.
 */
public class AuthInitializationRetryTest {

    @Test
    public void anImmediatelyFailedInitializationIsRetriedOnTheNextConnection() throws Exception {
        FailThenSucceedAuthentication body = new FailThenSucceedAuthentication();
        V5BinaryAuthenticationDriver driver = new V5BinaryAuthenticationDriver(body);

        AuthenticationExchange first = driver.newAuthenticationExchange("broker-1");
        assertThatThrownBy(() -> first.getAuthDataAsync().get(30, TimeUnit.SECONDS))
                .as("the first attempt surfaces the initialization failure")
                .hasRootCauseMessage("IdP not reachable yet");

        // The retry is the point: a cached failure would fail this one identically.
        AuthenticationExchange second = driver.newAuthenticationExchange("broker-1");
        assertThat(second.getAuthDataAsync().get(30, TimeUnit.SECONDS)).isNotNull();
        assertThat(body.initializeCalls).as("the second connection re-ran initialization").hasValue(2);
    }

    /** Fails initialization once, with an already-completed failed future, then succeeds. */
    private static final class FailThenSucceedAuthentication implements Authentication, BinaryAuthDataProvider {

        private final AtomicInteger initializeCalls = new AtomicInteger();

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            if (initializeCalls.incrementAndGet() == 1) {
                return CompletableFuture.failedFuture(new IllegalStateException("IdP not reachable yet"));
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> Optional<T> capability(Class<T> capabilityClass) {
            return capabilityClass.isInstance(this) ? Optional.of(capabilityClass.cast(this)) : Optional.empty();
        }

        @Override
        public String authMethodName() {
            return "fail-then-succeed";
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
            return CompletableFuture.completedFuture(new BinaryAuthData(new byte[]{1}));
        }

        @Override
        public void close() {
        }
    }
}
