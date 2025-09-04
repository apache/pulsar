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

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link AuthenticationOAuth2 with JwtBearerFlow}.
 */
public class AuthenticationOAuth2JwtBearerConfigTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    private AuthenticationOAuth2 auth;

    @BeforeMethod
    public void before() {
        MockClock clock = new MockClock(Instant.EPOCH, ZoneOffset.UTC);
        Flow flow = mock(JwtBearerFlow.class);
        this.auth = new AuthenticationOAuth2(flow, clock);
    }

    private static Map<String, String> getMinimalAuthConfig() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "jwt_bearer");
        return params;
    }

    private Map<String, String> getMinimalWorkingAuthConfig() {
        Map<String, String> params = getMinimalAuthConfig();
        params.put(JwtBearerFlow.CONFIG_PARAM_ISSUER_URL, "http://localhost/.well-known/openid-configuration");
        params.put(JwtBearerFlow.CONFIG_PARAM_AUDIENCE, "http://localhost");
        params.put(JwtBearerFlow.CONFIG_PARAM_KEY_FILE, "data:base64,e30="); // empty private key

        return params;
    }

    private static void configureAuth(AuthenticationOAuth2 auth, Map<String, String> params) throws Exception {
        String authParams = mapper.writeValueAsString(params);
        auth.configure(authParams);
    }

    @Test
    public void testGetAuthMethodName() throws Exception {
        Map<String, String> params = getMinimalWorkingAuthConfig();
        configureAuth(auth, params);

        assertNotNull(this.auth.flow);
        assertEquals(this.auth.getAuthMethodName(), "token");
    }


    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Unsupported.*")
    public void testUnknownType() throws Exception {
        Map<String, String> params = getMinimalAuthConfig();
        params.put("type", "garbage");
        configureAuth(auth, params);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Required.*")
    public void testMissingParams() throws Exception {
        Map<String, String> params = getMinimalAuthConfig();
        configureAuth(auth, params);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Required.*")
    public void testConfigureRequired() throws Exception {
        Map<String, String> params = getMinimalAuthConfig();
        configureAuth(auth, params);

        this.auth.configure("{}");
    }

    @Test
    public void testConfigureWithOptionalParams() throws Exception {
        Map<String, String> params = getMinimalWorkingAuthConfig();
        params.put(JwtBearerFlow.CONFIG_PARAM_TOKEN_TTL, "600000");
        params.put(JwtBearerFlow.CONFIG_PARAM_SIGNATURE_ALGORITHM, "RS256");
        configureAuth(auth, params);

        assertNotNull(this.auth.flow);
        assertEquals(this.auth.getAuthMethodName(), "token");
    }

}
