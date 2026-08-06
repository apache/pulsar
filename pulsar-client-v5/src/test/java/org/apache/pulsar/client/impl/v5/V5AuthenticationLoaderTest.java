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
package org.apache.pulsar.client.impl.v5;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.SinglePassAuthentication;
import org.apache.pulsar.client.impl.v5.auth.LegacyV4AuthenticationAdapter;
import org.apache.pulsar.client.impl.v5.auth.V5ToV4AuthenticationAdapter;
import org.testng.annotations.Test;

/**
 * Verifies the string-config reflective load path (PIP-478 In-Scope #2): a v5-native
 * {@link Authentication} deployed by class name is instantiated, configured with the parsed
 * {@code authParams}, and drives the Pulsar binary transport through the
 * {@link V5ToV4AuthenticationAdapter} — where before it was blind-cast to the v4 SPI and threw
 * {@link ClassCastException}. A legacy v4 class still loads through the {@link LegacyV4AuthenticationAdapter}.
 */
public class V5AuthenticationLoaderTest {

    /** A v5-native single-pass plugin, loadable by class name (public + public no-arg ctor for reflection). */
    public static final class FakeV5SinglePass implements SinglePassAuthentication {

        private volatile Map<String, String> configuredParams;
        private volatile boolean initialized;

        public FakeV5SinglePass() {
        }

        @Override
        public void configure(Map<String, String> authParams) {
            this.configuredParams = Map.copyOf(authParams);
        }

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            this.initialized = true;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String authMethodName() {
            return "fake-v5";
        }

        @Override
        public CompletableFuture<BinaryAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
            String user = configuredParams == null ? "" : configuredParams.getOrDefault("user", "");
            return CompletableFuture.completedFuture(new BinaryAuthData(("token:" + user).getBytes(UTF_8)));
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
            return CompletableFuture.completedFuture(HttpAuthHeaders.of("Authorization", "Bearer fake"));
        }
    }

    /** A minimal legacy v4 credential plugin, loadable by class name. */
    @SuppressWarnings("deprecation")
    public static final class FakeV4Plugin implements org.apache.pulsar.client.api.Authentication {

        public FakeV4Plugin() {
        }

        @Override
        public String getAuthMethodName() {
            return "fake-v4";
        }

        @Override
        public AuthenticationDataProvider getAuthData() {
            return null;
        }

        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void v5NativePluginLoadedByNameIsConfiguredAndDrivesBinaryTransport() throws Exception {
        Authentication auth = V5AuthenticationLoader.create(FakeV5SinglePass.class.getName(),
                "user:alice,realm:test");

        // The v5-native plugin is returned directly (not blind-cast to v4) and configure() ran with the
        // parsed key:val params.
        assertThat(auth).isInstanceOf(FakeV5SinglePass.class);
        assertThat(((FakeV5SinglePass) auth).configuredParams)
                .containsEntry("user", "alice")
                .containsEntry("realm", "test");

        // It drives the binary transport through the v5->v4 adapter that ClientCnx consumes.
        V5ToV4AuthenticationAdapter adapter = new V5ToV4AuthenticationAdapter(auth);
        adapter.start();
        assertThat(((FakeV5SinglePass) auth).initialized).isTrue();
        assertThat(adapter.getAuthMethodName()).isEqualTo("fake-v5");
        AuthenticationDataProvider data = adapter.getAuthData("broker-1.example.com");
        assertThat(data.getCommandData()).isEqualTo("token:alice");
    }

    @Test
    public void v5NativePluginAcceptsJsonParams() throws Exception {
        Authentication auth = V5AuthenticationLoader.create(FakeV5SinglePass.class.getName(),
                "{\"user\":\"bob\"}");
        assertThat(auth).isInstanceOf(FakeV5SinglePass.class);
        assertThat(((FakeV5SinglePass) auth).configuredParams).containsEntry("user", "bob");
    }

    @Test
    public void v5NativePluginLoadedByNameWithParamsMap() throws Exception {
        Authentication auth = V5AuthenticationLoader.create(FakeV5SinglePass.class.getName(),
                Map.of("user", "carol"));
        assertThat(auth).isInstanceOf(FakeV5SinglePass.class);
        assertThat(((FakeV5SinglePass) auth).configuredParams).containsEntry("user", "carol");
    }

    @Test
    public void v4PluginStillLoadsThroughLegacyAdapter() throws Exception {
        Authentication auth = V5AuthenticationLoader.create(FakeV4Plugin.class.getName(), "");

        // A v4 class is bridged, not returned raw: it is a LegacyV4AuthenticationAdapter wrapping the v4
        // plugin, so unwrapV4 recovers the original v4 instance.
        assertThat(auth).isNotInstanceOf(FakeV5SinglePass.class);
        assertThat(LegacyV4AuthenticationAdapter.unwrapV4(auth))
                .get()
                .isInstanceOf(FakeV4Plugin.class);
    }

    @Test
    public void blankClassNameLoadsDisabledAuthentication() throws Exception {
        Authentication auth = V5AuthenticationLoader.create("", "");
        // Blank routes to the v4 path, which yields AuthenticationDisabled bridged through the adapter.
        assertThat(LegacyV4AuthenticationAdapter.unwrapV4(auth))
                .get()
                .isInstanceOf(org.apache.pulsar.client.impl.auth.AuthenticationDisabled.class);
    }
}
