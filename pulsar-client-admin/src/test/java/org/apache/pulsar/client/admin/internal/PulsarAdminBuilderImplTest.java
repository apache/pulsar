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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link PulsarAdminBuilder}.
 */
public class PulsarAdminBuilderImplTest {

    private static final String MOCK_AUTH_SECRET_PLUGIN_CLASS = MockAuthenticationSecret.class.getName();

    private static final String AUTH_PLUGIN_CLASS_PROP = "authPluginClassName";

    private static final String AUTH_PARAMS_PROP = "authParams";

    private static final String AUTH_PARAM_MAP_PROP = "authParamMap";

    @Test
    public void testBuildFailsWhenServiceUrlNotSet() {
        assertThatIllegalArgumentException().isThrownBy(() -> PulsarAdmin.builder().build())
                        .withMessageContaining("Service URL needs to be specified");
    }

    @Test
    public void testGetPropertiesFromConf() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("serviceUrl", "pulsar://localhost:6650");
        config.put("requestTimeoutMs", 10);
        config.put("autoCertRefreshSeconds", 20);
        config.put("connectionTimeoutMs", 30);
        config.put("readTimeoutMs", 40);
        config.put("maxConnectionsPerHost", 50);
        PulsarAdminBuilder adminBuilder = PulsarAdmin.builder().loadConf(config);
        @Cleanup
        PulsarAdminImpl admin = (PulsarAdminImpl) adminBuilder.build();
        ClientConfigurationData clientConfigData = admin.getClientConfigData();
        Assert.assertEquals(clientConfigData.getRequestTimeoutMs(), 10);
        Assert.assertEquals(clientConfigData.getAutoCertRefreshSeconds(), 20);
        Assert.assertEquals(clientConfigData.getConnectionTimeoutMs(), 30);
        Assert.assertEquals(clientConfigData.getReadTimeoutMs(), 40);
        Assert.assertEquals(clientConfigData.getConnectionsPerBroker(), 50);
    }

    @Test
    public void testLoadConfSetsAuthUsingAuthParamsProp() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAMS_PROP, secretAuthParams("pass1"));
        Authentication auth = createAdminAndGetAuth(confProps);
        assertAuthWithSecret(auth, "pass1");
    }

    @Test
    public void testLoadConfSetsAuthUsingAuthParamMapProp() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAM_MAP_PROP, secretAuthParamMap("pass1"));
        Authentication auth = createAdminAndGetAuth(confProps);
        assertAuthWithSecret(auth, "pass1");
    }

    @Test
    public void testLoadConfSetsAuthUsingAuthParamsPropWhenBothPropsAvailable() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAMS_PROP, secretAuthParams("pass1"));
        confProps.put(AUTH_PARAM_MAP_PROP, secretAuthParamMap("pass2"));
        Authentication auth = createAdminAndGetAuth(confProps);
        assertAuthWithSecret(auth, "pass1");
    }

    private void assertAuthWithSecret(Authentication authentication, String secret) {
        assertThat(authentication).isInstanceOfSatisfying(MockAuthenticationSecret.class,
                (auth) -> assertThat(auth.getSecret()).isEqualTo(secret));
    }

    @Test
    public void testLoadConfAuthNotSetWhenNoPropsAvailable() {
        Authentication auth = createAdminAndGetAuth(Collections.emptyMap());
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenEmptyAuthParamsSpecified() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAMS_PROP, "");
        Authentication auth = createAdminAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenNullAuthParamsSpecified() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAMS_PROP, null);
        Authentication auth = createAdminAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenNullParamMapSpecified() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        confProps.put(AUTH_PARAM_MAP_PROP, null);
        Authentication auth = createAdminAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenOnlyPluginClassNameAvailable() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PLUGIN_CLASS_PROP, MOCK_AUTH_SECRET_PLUGIN_CLASS);
        Authentication auth = createAdminAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenOnlyAuthParamsAvailable() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PARAMS_PROP, secretAuthParams("pass1"));
        Authentication auth = createAdminAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    @Test
    public void testLoadConfAuthNotSetWhenOnlyAuthParamMapAvailable() {
        Map<String, Object> confProps = new HashMap<>();
        confProps.put(AUTH_PARAM_MAP_PROP, secretAuthParamMap("pass2"));
        Authentication auth = createAdminAndGetAuth(confProps);
        assertThatAuthIsNotSet(auth);
    }

    private void assertThatAuthIsNotSet(Authentication authentication) {
        // getAuthentication() returns disabled when null
        assertThat(authentication).isInstanceOf(AuthenticationDisabled.class);
    }

    @SneakyThrows
    private Authentication createAdminAndGetAuth(Map<String, Object> confProps) {
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080")
                .loadConf(confProps).build()) {
            return ((PulsarAdminImpl) admin).auth;
        }
    }

    @Test
    public void testClientDescription() throws PulsarClientException {
        @Cleanup PulsarAdmin ignored =
                PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").description("forked").build();
    }

    @Test
    public void testClientBuildWithSharedResources() throws PulsarClientException {
        PulsarClientSharedResources sharedResources = PulsarClientSharedResources.builder()
                .configureEventLoop(eventLoopGroupConfig -> {
                    eventLoopGroupConfig
                            .name("testEventLoop")
                            .numberOfThreads(20);
                })
                .configureDnsResolver(dnsResolverConfig -> {
                    dnsResolverConfig.localAddress(new InetSocketAddress(0));
                })
                .configureTimer(timerConfig -> {
                    timerConfig.name("testTimer").tickDuration(100, TimeUnit.MILLISECONDS);
                })
                .build();
        // create two adminClients and check if they share the same event loop group and netty timer
        @Cleanup
        PulsarAdminImpl pulsarAdminImpl1 =
                (PulsarAdminImpl) PulsarAdmin.builder()
                        .serviceHttpUrl("http://localhost:8080")
                        .sharedResources(sharedResources)
                        .build();
        @Cleanup
        PulsarAdminImpl pulsarAdminImpl2 =
                (PulsarAdminImpl) PulsarAdmin.builder()
                        .serviceHttpUrl("http://localhost:8080")
                        .sharedResources(sharedResources)
                        .build();

        EventLoopGroup eventLoopGroup1 =
                pulsarAdminImpl1.getAsyncHttpConnector().getHttpClient().getConfig().getEventLoopGroup();
        EventLoopGroup eventLoopGroup2 =
                pulsarAdminImpl2.getAsyncHttpConnector().getHttpClient().getConfig().getEventLoopGroup();
        Timer nettyTimer1 = pulsarAdminImpl1.getAsyncHttpConnector().getHttpClient().getConfig().getNettyTimer();
        Timer nettyTimer2 = pulsarAdminImpl2.getAsyncHttpConnector().getHttpClient().getConfig().getNettyTimer();
        assertThat(eventLoopGroup1).isSameAs(eventLoopGroup2);
        assertThat(nettyTimer1).isSameAs(nettyTimer2);
        sharedResources.close();
    }

    @Test
    public void testClientBuildWithSharedDnsResolverOnly() throws PulsarClientException {
        PulsarClientSharedResources sharedResources = PulsarClientSharedResources.builder()
                .shareConfigured()
                .configureDnsResolver(dnsResolverConfig -> {
                    dnsResolverConfig.localAddress(new InetSocketAddress(0));
                })
                .build();

        @Cleanup
        PulsarAdminImpl pulsarAdminImpl1 =
                (PulsarAdminImpl) PulsarAdmin.builder()
                        .serviceHttpUrl("http://localhost:8080")
                        .sharedResources(sharedResources)
                        .build();
        @Cleanup
        PulsarAdminImpl pulsarAdminImpl2 =
                (PulsarAdminImpl) PulsarAdmin.builder()
                        .serviceHttpUrl("http://localhost:8080")
                        .sharedResources(sharedResources)
                        .build();

        EventLoopGroup eventLoopGroup1 =
                pulsarAdminImpl1.getAsyncHttpConnector().getHttpClient().getConfig().getEventLoopGroup();
        EventLoopGroup eventLoopGroup2 =
                pulsarAdminImpl2.getAsyncHttpConnector().getHttpClient().getConfig().getEventLoopGroup();
        Timer nettyTimer1 = pulsarAdminImpl1.getAsyncHttpConnector().getHttpClient().getConfig().getNettyTimer();
        Timer nettyTimer2 = pulsarAdminImpl2.getAsyncHttpConnector().getHttpClient().getConfig().getNettyTimer();
        NameResolver<InetAddress> nameResolver1 = pulsarAdminImpl1.getAsyncHttpConnector().getNameResolver();
        NameResolver<InetAddress> nameResolver2 = pulsarAdminImpl2.getAsyncHttpConnector().getNameResolver();

        // test eventLoop will be created when dnsResolver is configured
        assertThat(eventLoopGroup1).isNotNull();
        assertThat(eventLoopGroup2).isNotNull();
        assertThat(eventLoopGroup2).isNotSameAs(eventLoopGroup1);

        // timer will not be created when timer is not configured
        assertThat(nettyTimer1).isSameAs(nettyTimer2).isNull();

        assertThat(nameResolver1).isNotNull();
        assertThat(nameResolver2).isNotNull();

        // test eventLoop will shut down when AsyncHttpConnector is closed
        pulsarAdminImpl1.getAsyncHttpConnector().close();
        assertThat(eventLoopGroup1.isShuttingDown()).isTrue();

        sharedResources.close();
    }

    @Test
    public void testClientDescriptionLengthExceed64() {
        String longDescription = "a".repeat(65);
        assertThatThrownBy(() -> {
            @Cleanup PulsarAdmin ignored =
                    PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").description(longDescription).build();
        }).isInstanceOf(IllegalArgumentException.class);
    }

    private String secretAuthParams(String secret) {
        return String.format("{\"secret\":\"%s\"}", secret);
    }

    private Map<String, String> secretAuthParamMap(String secret) {
        return Collections.singletonMap("secret", secret);
    }

    public static class MockAuthenticationSecret implements Authentication, EncodedAuthenticationParameterSupport {

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
