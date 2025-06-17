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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import no.nav.security.mock.oauth2.MockOAuth2Server;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link AuthenticationOAuth2}.
 */
public class AuthenticationOAuth2JwtBearerTest {
    static final String RSA_PRIVATE_KEY = "./src/test/resources/crypto_rsa_private.key";

    private static final ObjectMapper mapper = new ObjectMapper();

    private AuthenticationOAuth2 auth;

    MockOAuth2Server server;
    String wellKnownUrl;

    @BeforeMethod
    public void before() throws Exception {
        server = new MockOAuth2Server();
        server.start();
        wellKnownUrl = server.wellKnownUrl("default").toString();
        this.auth = new AuthenticationOAuth2();
    }

    @AfterMethod
    public void after() {
        if (server != null) {
            server.shutdown();
        }
    }

    private static Map<String, String> getWorkingAuthConfig(String wellKnownUrl) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("type", "jwt_bearer");
        params.put(JwtBearerFlow.CONFIG_PARAM_ISSUER_URL, wellKnownUrl);
        params.put(JwtBearerFlow.CONFIG_PARAM_AUDIENCE, "http://localhost");
        String privateKey = Paths.get(RSA_PRIVATE_KEY).toUri().toURL().toString();

        KeyFile kf = new KeyFile();
        kf.setType("jwt_bearer");
        kf.setIssuerUrl(wellKnownUrl);
        kf.setClientId("test-client-id");
        kf.setClientEmail("test-client-email@noop.com");
        kf.setClientSecret(privateKey);

        Path filePath = Files.createTempFile("keyfile", ".json");
        filePath.toFile().deleteOnExit();
        FileUtils.writeStringToFile(filePath.toFile(), kf.toJson(), StandardCharsets.UTF_8);

        params.put(JwtBearerFlow.CONFIG_PARAM_KEY_FILE, filePath.toUri().toURL().toString());

        return params;
    }

    private static void configureAuth(AuthenticationOAuth2 auth, Map<String, String> params) throws Exception {
        String authParams = mapper.writeValueAsString(params);
        auth.configure(authParams);
    }

    @Test
    public void testE2e() throws Exception {
        Map<String, String> params = getWorkingAuthConfig(wellKnownUrl);
        configureAuth(auth, params);

        assertNotNull(this.auth.flow);
        assertEquals(this.auth.getAuthMethodName(), "token");

        AuthenticationDataProvider data;
        data = this.auth.getAuthData();
        assertNotNull(data.getCommandData());
    }

}
