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
package org.apache.pulsar.client.impl.auth.oauth2;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenResult;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link AuthenticationOAuth2}.
 */
public class AuthenticationOAuth2Test {
    private static final String TEST_ACCESS_TOKEN = "x.y.z";
    private static final int TEST_EXPIRES_IN = 60;

    private MockClock clock;
    private Flow flow;
    private AuthenticationOAuth2 auth;

    @BeforeMethod
    public void before() {
        this.clock = new MockClock(Instant.EPOCH, ZoneOffset.UTC);
        this.flow = mock(Flow.class);
        this.auth = new AuthenticationOAuth2(flow, this.clock, 1, null);
    }

    @Test
    public void testGetAuthMethodName() {
        assertEquals(this.auth.getAuthMethodName(), "token");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*No.*")
    public void testConfigureNoParams() throws Exception {
        this.auth.configure("");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Malformed.*")
    public void testConfigureMalformed() throws Exception {
        this.auth.configure("{garbage}");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Required.*")
    public void testConfigureRequired() throws Exception {
        this.auth.configure("{}");
    }

    @Test
    public void testConfigure() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("type", "client_credentials");
        params.put("privateKey", "data:base64,e30=");
        params.put("issuerUrl", "http://localhost");
        params.put("audience", "http://localhost");
        params.put("scope", "http://localhost");
        params.put("wellKnownMetadataPath", "/.well-known/custom-path");
        ObjectMapper mapper = new ObjectMapper();
        String authParams = mapper.writeValueAsString(params);
        this.auth.configure(authParams);
        assertNotNull(this.auth.flow);
    }

    @Test
    public void testConfigureWithoutOptionalParams() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("type", "client_credentials");
        params.put("privateKey", "data:base64,e30=");
        params.put("issuerUrl", "http://localhost");
        ObjectMapper mapper = new ObjectMapper();
        String authParams = mapper.writeValueAsString(params);
        this.auth.configure(authParams);
        assertNotNull(this.auth.flow);
    }

    // ----- configure() via default constructor -----

    @Test
    public void testConfigureViaDefaultConstructorSetsFlow() throws Exception {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure(minimalCredentialsJson());
        assertNotNull(auth.flow);
    }

    @Test
    public void testConfigureViaDefaultConstructorDefaultEarlyRefreshIsDisabled() throws Exception {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure(minimalCredentialsJson());
        assertEquals(auth.earlyTokenRefreshPercent, (double) AuthenticationOAuth2.EARLY_TOKEN_REFRESH_PERCENT_DEFAULT);
    }

    @Test
    public void testConfigureViaDefaultConstructorEarlyRefreshDecimal() throws Exception {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure(credentialsJsonWithEarlyRefresh("0.8"));
        assertEquals(auth.earlyTokenRefreshPercent, 0.8);
    }

    @Test
    public void testConfigureViaDefaultConstructorEarlyRefreshInteger() throws Exception {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure(credentialsJsonWithEarlyRefresh("80"));
        assertEquals(auth.earlyTokenRefreshPercent, 0.8);
    }

    @Test
    public void testConfigureViaDefaultConstructorEarlyRefreshIntegerDisabled() throws Exception {
        // Integer 100 → 1.0, meaning disabled
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure(credentialsJsonWithEarlyRefresh("100"));
        assertEquals(auth.earlyTokenRefreshPercent, 1.0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConfigureViaDefaultConstructorEarlyRefreshZero() throws Exception {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure(credentialsJsonWithEarlyRefresh("0"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testConfigureViaDefaultConstructorEarlyRefreshInvalid() throws Exception {
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure(credentialsJsonWithEarlyRefresh("not-a-number"));
    }

    @Test
    public void testConfigureViaDefaultConstructorEarlyRefreshTriggersBackgroundRefresh() throws Exception {
        // Verify that when early refresh is enabled via configure(), the background scheduler
        // is activated and actually calls authenticate() before token expiry.
        AuthenticationOAuth2 auth = new AuthenticationOAuth2();
        auth.configure(credentialsJsonWithEarlyRefresh("10")); // 10% → refresh at 100ms of a 1s token
        // Replace the flow with a mock after configure() has set it up
        auth.flow = this.flow;
        TokenResult tr = TokenResult.builder().accessToken(TEST_ACCESS_TOKEN).expiresIn(1).build();
        doReturn(tr).when(this.flow).authenticate();

        auth.getAuthData();
        // Give the background scheduler time to trigger at least one additional refresh
        Thread.sleep(500);
        auth.close();
        verify(this.flow, atLeast(2)).authenticate();
    }

    // ----- parseEarlyRefreshPercent -----

    @Test
    public void testParseEarlyRefreshPercentDecimal() {
        assertEquals(AuthenticationOAuth2.parseEarlyRefreshPercent("0.8"), 0.8);
        assertEquals(AuthenticationOAuth2.parseEarlyRefreshPercent("0.5"), 0.5);
        assertEquals(AuthenticationOAuth2.parseEarlyRefreshPercent("1.0"), 1.0);
    }

    @Test
    public void testParseEarlyRefreshPercentInteger() {
        assertEquals(AuthenticationOAuth2.parseEarlyRefreshPercent("80"), 0.8);
        assertEquals(AuthenticationOAuth2.parseEarlyRefreshPercent("50"), 0.5);
        assertEquals(AuthenticationOAuth2.parseEarlyRefreshPercent("100"), 1.0);
        assertEquals(AuthenticationOAuth2.parseEarlyRefreshPercent("1"), 0.01);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseEarlyRefreshPercentZeroInteger() {
        AuthenticationOAuth2.parseEarlyRefreshPercent("0");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseEarlyRefreshPercentInvalid() {
        AuthenticationOAuth2.parseEarlyRefreshPercent("not-a-number");
    }

    // ----- helpers -----

    private static String minimalCredentialsJson() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("type", "client_credentials");
        params.put("privateKey", "data:base64,e30=");
        params.put("issuerUrl", "http://localhost");
        return new ObjectMapper().writeValueAsString(params);
    }

    private static String credentialsJsonWithEarlyRefresh(String earlyRefreshValue) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("type", "client_credentials");
        params.put("privateKey", "data:base64,e30=");
        params.put("issuerUrl", "http://localhost");
        params.put(AuthenticationOAuth2.CONFIG_PARAM_EARLY_TOKEN_REFRESH_PERCENT, earlyRefreshValue);
        return new ObjectMapper().writeValueAsString(params);
    }

    @Test
    public void testStart() throws Exception {
        this.auth.start();
        verify(this.flow).initialize();
    }

    @Test
    public void testGetAuthDataNoEarlyRefresh() throws Exception {
        AuthenticationDataProvider data;
        TokenResult tr = TokenResult.builder().accessToken(TEST_ACCESS_TOKEN).expiresIn(TEST_EXPIRES_IN).build();
        doReturn(tr).when(this.flow).authenticate();
        data = this.auth.getAuthData();
        verify(this.flow, times(1)).authenticate();
        assertEquals(data.getCommandData(), tr.getAccessToken());

        // cache hit
        data = this.auth.getAuthData();
        verify(this.flow, times(1)).authenticate();
        assertEquals(data.getCommandData(), tr.getAccessToken());

        // cache miss (have to move passed expiration b/c we refresh when token is expired now)
        // NOTE: this works because the token uses the mocked clock.
        clock.advance(Duration.ofSeconds(TEST_EXPIRES_IN + 1));
        data = this.auth.getAuthData();
        verify(this.flow, times(2)).authenticate();
        assertEquals(data.getCommandData(), tr.getAccessToken());
    }

    // This test skips the early refresh logic and just ensures that if the class were to somehow fail
    // to refresh the token before expiration, the caller will get one final attempt at calling authenticate
    @Test
    public void testGetAuthDataWithEarlyRefresh() throws Exception {
        @Cleanup AuthenticationOAuth2 auth = new AuthenticationOAuth2(flow, this.clock, 0.8, null);
        AuthenticationDataProvider data;
        TokenResult tr = TokenResult.builder().accessToken(TEST_ACCESS_TOKEN).expiresIn(TEST_EXPIRES_IN).build();
        doReturn(tr).when(this.flow).authenticate();
        data = auth.getAuthData();
        verify(this.flow, times(1)).authenticate();
        assertEquals(data.getCommandData(), tr.getAccessToken());

        // cache hit
        data = auth.getAuthData();
        verify(this.flow, times(1)).authenticate();
        assertEquals(data.getCommandData(), tr.getAccessToken());

        // cache miss (have to move passed expiration b/c we refresh when token is expired now)
        clock.advance(Duration.ofSeconds(TEST_EXPIRES_IN + 1));
        data = auth.getAuthData();
        verify(this.flow, times(2)).authenticate();
        assertEquals(data.getCommandData(), tr.getAccessToken());
    }

    // This test ensures that the early token refresh actually calls the authenticate method in the background.
    @Test
    public void testEarlyTokenRefreshCallsAuthenticate() throws Exception {
        @Cleanup AuthenticationOAuth2 auth = new AuthenticationOAuth2(flow, this.clock, 0.1, null);
        TokenResult tr = TokenResult.builder().accessToken(TEST_ACCESS_TOKEN).expiresIn(1).build();
        doReturn(tr).when(this.flow).authenticate();
        // Initialize the flow
        auth.getAuthData();
        // Give the auth token refresh a chance to run multiple times
        Thread.sleep(1000);
        auth.close();
        verify(this.flow, atLeast(2)).authenticate();
        verify(this.flow).close();
    }

    // This test ensures scheduler is used when passed in
    @Test
    public void testEarlyTokenRefreshCallsAuthenticateWithParameterizedScheduler() throws Exception {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        @Cleanup AuthenticationOAuth2 auth = new AuthenticationOAuth2(flow, this.clock, 0.1, scheduler);
        TokenResult tr = TokenResult.builder().accessToken(TEST_ACCESS_TOKEN).expiresIn(1).build();
        doReturn(tr).when(this.flow).authenticate();
        // Initialize the flow and trigger scheduling
        auth.getAuthData();
        verify(scheduler, times(1)).execute(any(Runnable.class));
        // Close and verify that the passed in scheduler isn't shutdown
        auth.close();
        verify(this.flow).close();
        verify(scheduler, times(0)).shutdownNow();
        verify(scheduler, times(2)).execute(any(Runnable.class));
    }

    @Test
    public void testMetadataResolver() throws MalformedURLException {
        URL url = DefaultMetadataResolver.getWellKnownMetadataUrl(
                URI.create("http://localhost/path/oauth").toURL(),
                null);
        assertEquals("http://localhost/path/oauth/.well-known/openid-configuration", url.toString());

        // custom wellKnownMetadataPath with full well-known prefix
        URL customUrl = DefaultMetadataResolver.getWellKnownMetadataUrl(
                URI.create("http://localhost/path/oauth").toURL(),
                "/.well-known/custom-path");
        assertEquals("http://localhost/.well-known/custom-path/path/oauth", customUrl.toString());

        // null wellKnownMetadataPath (should use default)
        URL customUrl2 = DefaultMetadataResolver.getWellKnownMetadataUrl(
                URI.create("http://localhost/path/oauth").toURL(),
                null);
        assertEquals("http://localhost/path/oauth/.well-known/openid-configuration", customUrl2.toString());

        // empty wellKnownMetadataPath (should use default)
        URL customUrl3 = DefaultMetadataResolver.getWellKnownMetadataUrl(
                URI.create("http://localhost/path/oauth").toURL(),
                "");
        assertEquals("http://localhost/path/oauth/.well-known/openid-configuration", customUrl3.toString());

        // using RFC8414 OAuth2 metadata path
        URL oauthUrl = DefaultMetadataResolver.getWellKnownMetadataUrl(
                URI.create("http://localhost/path/oauth").toURL(),
                DefaultMetadataResolver.OAUTH_WELL_KNOWN_METADATA_PATH);
        assertEquals("http://localhost/.well-known/oauth-authorization-server/path/oauth", oauthUrl.toString());

        // test with issuer URL without path
        URL oauthUrlNoPath = DefaultMetadataResolver.getWellKnownMetadataUrl(
                URI.create("http://localhost").toURL(),
                DefaultMetadataResolver.OAUTH_WELL_KNOWN_METADATA_PATH);
        assertEquals("http://localhost/.well-known/oauth-authorization-server", oauthUrlNoPath.toString());
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*Metadata path must start with.*")
    public void testMetadataResolverWithInvalidPath() throws MalformedURLException {
        DefaultMetadataResolver.getWellKnownMetadataUrl(
                URI.create("http://localhost/path/oauth").toURL(),
                "/custom-path");
    }

    @Test
    public void testClose() throws Exception {
        this.auth.close();
        verify(this.flow).close();
        assertThrows(PulsarClientException.AlreadyClosedException.class, () -> this.auth.getAuthData());
    }
}
