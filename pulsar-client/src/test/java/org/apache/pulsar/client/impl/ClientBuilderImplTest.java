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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.testng.annotations.Test;

public class ClientBuilderImplTest {

    private static final String MOCK_AUTH_SECRET_PLUGIN_CLASS = MockAuthenticationSecret.class.getName();

    private static final String AUTH_PLUGIN_CLASS_PROP = "authPluginClassName";

    private static final String AUTH_PARAMS_PROP = "authParams";

    private static final String AUTH_PARAM_MAP_PROP = "authParamMap";

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithServiceUrlAndServiceUrlProviderNotSet() throws PulsarClientException {
        PulsarClient.builder().build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithNullServiceUrl() throws PulsarClientException {
        PulsarClient.builder().serviceUrl(null).build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithNullServiceUrlProvider() throws PulsarClientException {
        PulsarClient.builder().serviceUrlProvider(null).build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithServiceUrlAndServiceUrlProvider() throws PulsarClientException {
        PulsarClient.builder().serviceUrlProvider(new ServiceUrlProvider() {
            @Override
            public void initialize(PulsarClient client) {

            }

            @Override
            public String getServiceUrl() {
                return "pulsar://localhost:6650";
            }
        }).serviceUrl("pulsar://localhost:6650").build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithBlankServiceUrlInServiceUrlProvider() throws PulsarClientException {
        PulsarClient.builder().serviceUrlProvider(new ServiceUrlProvider() {
            @Override
            public void initialize(PulsarClient client) {

            }

            @Override
            public String getServiceUrl() {
                return "";
            }
        }).build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithIllegalMinusPort() throws PulsarClientException {
        PulsarClient.builder().dnsLookupBind("localhost", -1).build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testClientBuilderWithIllegalLargePort() throws PulsarClientException {
        PulsarClient.builder().dnsLookupBind("localhost", 65536).build();
    }


    // Tests for loadConf and authentication

    @Test
    public void testLoadConfSetsAuthUsingAuthParamsProp() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAMS_PROP, secretAuthParams("pass1"));
        Authentication auth = createClientAndGetAuth(confProps);
        assertAuthWithSecret(auth, "pass1");
    }

    @Test
    public void testLoadConfSetsAuthUsingAuthParamMapProp() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAM_MAP_PROP, secretAuthParamMap("pass1"));
        Authentication auth = createClientAndGetAuth(confProps);
        assertAuthWithSecret(auth, "pass1");
    }

    @Test
    public void testLoadConfSetsAuthUsingAuthParamsPropWhenBothPropsAvailable() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAMS_PROP, secretAuthParams("pass1"));
        confProps.put(AUTH_PARAM_MAP_PROP, secretAuthParamMap("pass2"));
        Authentication auth = createClientAndGetAuth(confProps);
        assertAuthWithSecret(auth, "pass1");
    }

    private void assertAuthWithSecret(Authentication authentication, String secret) {
        assertThat(authentication).isInstanceOfSatisfying(MockAuthenticationSecret.class,
                (auth) -> assertThat(auth.getSecret()).isEqualTo(secret));
    }

    @Test
    public void testLoadConfAuthNotSetWhenNoPropsAvailable() {
        Authentication auth = createClientAndGetAuth(Collections.emptyMap());
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenEmptyAuthParamsSpecified() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAMS_PROP, "");
        Authentication auth = createClientAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenNullAuthParamsSpecified() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAMS_PROP, null);
        Authentication auth = createClientAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenNullParamMapSpecified() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAM_MAP_PROP, null);
        Authentication auth = createClientAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenOnlyPluginClassNameAvailable() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        Authentication auth = createClientAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenOnlyAuthParamsAvailable() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PARAMS_PROP, secretAuthParams("pass1"));
        Authentication auth = createClientAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenOnlyAuthParamMapAvailable() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PARAM_MAP_PROP, secretAuthParamMap("pass2"));
        Authentication auth = createClientAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    private void assertThatAuthIsNotSet(Authentication authentication) {
        // getAuthentication() returns disabled when null
        assertThat(authentication).isInstanceOf(AuthenticationDisabled.class);
    }

    @SneakyThrows
    private Authentication createClientAndGetAuth(Map<String, Object> confProps) {
        try (PulsarClient client = PulsarClient.builder().serviceUrl("http://localhost:8080").loadConf(confProps).build()) {
            return ((PulsarClientImpl)client).conf.getAuthentication();
        }
    }

    private String secretAuthParams(String secret) {
        return String.format("{\"secret\":\"%s\"}", secret);
    }

    private Map<String, String> secretAuthParamMap(String secret) {
        return Collections.singletonMap("secret", secret);
    }

    static public class MockAuthenticationSecret implements Authentication, EncodedAuthenticationParameterSupport {

        private String secret;

        @Override
        public String getAuthMethodName() {
            return "mock-secret";
        }

        @Override
        public AuthenticationDataProvider getAuthData() throws PulsarClientException {
            return null;
        }

        @Override
        public void configure(Map<String, String> authParams) {
            configure(new Gson().toJson(authParams));
        }

        @Override
        public void configure(String encodedAuthParamString) {
            JsonObject params = new Gson().fromJson(encodedAuthParamString, JsonObject.class);
            secret = params.get("secret").getAsString();
        }

        @Override
        public void start() throws PulsarClientException {
        }

        @Override
        public void close() throws IOException {
        }

        public String getSecret() {
            return secret;
        }
    }

}
