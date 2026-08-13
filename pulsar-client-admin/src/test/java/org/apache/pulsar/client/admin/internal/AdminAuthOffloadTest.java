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
package org.apache.pulsar.client.admin.internal;

import static org.assertj.core.api.Assertions.assertThat;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.Test;

/**
 * PIP-478: the admin's v4 authentication composition must not run on the thread that issued the admin
 * call.
 *
 * <p>{@code HttpClient} (the lookup path) has off-loaded the identical composition since the core
 * migration, naming the self-deadlock it avoids; {@code BaseResource} was the one place left where a
 * blocking {@code getAuthData()} — an OAuth2 or Athenz shim refreshing its credential over synchronous
 * HTTP — ran wherever the caller happened to be, which for a broker calling its own admin client is a
 * request-handling thread.
 */
public class AdminAuthOffloadTest {

    /**
     * A v4 plugin whose credential call records the thread it ran on, standing in for one that blocks
     * there.
     */
    private static class ThreadRecordingAuthentication implements Authentication {

        private final AtomicReference<Thread> getAuthDataThread = new AtomicReference<>();

        @Override
        public String getAuthMethodName() {
            return "thread-recording";
        }

        @Override
        public AuthenticationDataProvider getAuthData() {
            getAuthDataThread.set(Thread.currentThread());
            return new AuthenticationDataProvider() {
                @Override
                public boolean hasDataForHttp() {
                    return true;
                }

                @Override
                public Set<Map.Entry<String, String>> getHttpHeaders() {
                    return Set.of(Map.entry("X-Test", "value"));
                }
            };
        }

        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() throws PulsarClientException {
        }

        @Override
        public void close() {
        }
    }

    /** The minimal concrete resource needed to reach the protected header composition. */
    private static class TestResource extends BaseResource {
        TestResource(Authentication auth) {
            super(auth, 30_000L);
        }

        CompletableFuture<Map<String, String>> headers(URI uri) {
            return computeAuthHeaders(uri);
        }
    }

    @Test
    public void v4CredentialResolutionRunsOffTheCallerThread() throws Exception {
        ThreadRecordingAuthentication auth = new ThreadRecordingAuthentication();
        TestResource resource = new TestResource(auth);

        Map<String, String> headers = resource.headers(URI.create("http://broker.example:8080/admin/v2/clusters"))
                .get(30, TimeUnit.SECONDS);

        // The composition still produces the plugin's headers verbatim — off-loading must not change what
        // a third-party v4 plugin contributes.
        assertThat(headers).containsExactly(Map.entry("X-Test", "value"));
        assertThat(auth.getAuthDataThread.get())
                .as("the v4 credential must be resolved off the thread that issued the admin call")
                .isNotNull()
                .isNotSameAs(Thread.currentThread());
    }

    @Test
    public void aPluginContributingNoHttpDataStillYieldsNoHeaders() throws Exception {
        Authentication auth = new ThreadRecordingAuthentication() {
            @Override
            public AuthenticationDataProvider getAuthData() {
                return new AuthenticationDataProvider() {
                    @Override
                    public boolean hasDataForHttp() {
                        return false;
                    }
                };
            }
        };

        Map<String, String> headers = new TestResource(auth)
                .headers(URI.create("http://broker.example:8080/admin/v2/clusters")).get(30, TimeUnit.SECONDS);

        assertThat(headers).isNull();
    }
}
