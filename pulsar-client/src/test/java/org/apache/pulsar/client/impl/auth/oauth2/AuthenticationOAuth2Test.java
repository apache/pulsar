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
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
        ScheduledThreadPoolExecutor scheduler = mock(ScheduledThreadPoolExecutor.class);
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
        URL url = DefaultMetadataResolver.getWellKnownMetadataUrl(URI.create("http://localhost/path/oauth").toURL());
        assertEquals("http://localhost/path/oauth/.well-known/openid-configuration", url.toString());
    }

    @Test
    public void testClose() throws Exception {
        this.auth.close();
        verify(this.flow).close();
        assertThrows(PulsarClientException.AlreadyClosedException.class, () -> this.auth.getAuthData());
    }
}
