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
package org.apache.pulsar.client.impl.auth;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import static org.apache.pulsar.common.util.Codec.encode;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.athenz.auth.util.Crypto;
import com.yahoo.athenz.zts.RoleToken;
import com.yahoo.athenz.zts.ZTSClient;

import lombok.Cleanup;

public class AuthenticationAthenzTest {

    private AuthenticationAthenz auth;
    private static final String TENANT_DOMAIN = "test_tenant";
    private static final String TENANT_SERVICE = "test_service";

    class MockZTSClient extends ZTSClient {
        public MockZTSClient(String ztsUrl) {
            super(ztsUrl);
        }

        @Override
        public RoleToken getRoleToken(String domainName, String roleName, Integer minExpiryTime, Integer maxExpiryTime,
                boolean ignoreCache) {
            List<String> roles = new ArrayList<String>() {
                {
                    add("test_role");
                }
            };
            com.yahoo.athenz.auth.token.RoleToken roleToken = new com.yahoo.athenz.auth.token.RoleToken.Builder(
                    "Z1", domainName, roles).principal(String.format("%s.%s", TENANT_DOMAIN, TENANT_SERVICE))
                            .build();

            try {
                String ztsPrivateKey = new String(
                        Files.readAllBytes(Paths.get("./src/test/resources/zts_private.pem")));
                roleToken.sign(ztsPrivateKey);
            } catch (IOException e) {
                return null;
            }

            RoleToken token = new RoleToken();
            token.setToken(roleToken.getSignedToken());

            return token;
        }
    }

    @BeforeClass
    public void setup() throws Exception {
        String paramsStr = new String(Files.readAllBytes(Paths.get("./src/test/resources/authParams.json")));
        auth = new AuthenticationAthenz();
        auth.configure(paramsStr);
        // Set mock ztsClient which returns fixed token instead of fetching from ZTS server
        Field field = auth.getClass().getDeclaredField("ztsClient");
        field.setAccessible(true);
        field.set(auth, new MockZTSClient("dummy"));
    }

    @Test
    public void testGetAuthData() throws Exception {

        com.yahoo.athenz.auth.token.RoleToken roleToken = new com.yahoo.athenz.auth.token.RoleToken(
                auth.getAuthData().getCommandData());
        assertEquals(roleToken.getPrincipal(), String.format("%s.%s", TENANT_DOMAIN, TENANT_SERVICE));

        int count = 0;
        for (Map.Entry<String, String> header : auth.getAuthData().getHttpHeaders()) {
            if (header.getKey().equals(ZTSClient.getHeader())) {
                com.yahoo.athenz.auth.token.RoleToken roleTokenFromHeader = new com.yahoo.athenz.auth.token.RoleToken(
                        header.getValue());
                assertEquals(roleTokenFromHeader.getPrincipal(), String.format("%s.%s", TENANT_DOMAIN, TENANT_SERVICE));
                count++;
            }
        }
        assertEquals(count, 1);
    }

    @Test
    public void testZtsUrl() throws Exception {
        Field field = auth.getClass().getDeclaredField("ztsUrl");
        field.setAccessible(true);
        String ztsUrl = (String) field.get(auth);
        assertEquals(ztsUrl, "https://localhost:4443/");
    }

    @Test
    public void testLoadPrivateKeyBase64() throws Exception {
        try {
            String paramsStr = new String(Files.readAllBytes(Paths.get("./src/test/resources/authParams.json")));

            // load privatekey and encode it using base64
            ObjectMapper jsonMapper = ObjectMapperFactory.create();
            Map<String, String> authParamsMap = jsonMapper.readValue(paramsStr,
                    new TypeReference<HashMap<String, String>>() {
                    });
            String privateKeyContents = new String(Files.readAllBytes(Paths.get(authParamsMap.get("privateKey"))));
            authParamsMap.put("privateKey", "data:application/x-pem-file;base64,"
                    + new String(Base64.getEncoder().encode(privateKeyContents.getBytes())));

            AuthenticationAthenz authBase64 = new AuthenticationAthenz();
            authBase64.configure(jsonMapper.writeValueAsString(authParamsMap));

            PrivateKey privateKey = Crypto.loadPrivateKey(new File("./src/test/resources/tenant_private.pem"));
            Field field = authBase64.getClass().getDeclaredField("privateKey");
            field.setAccessible(true);
            PrivateKey key = (PrivateKey) field.get(authBase64);
            assertEquals(key, privateKey);
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testLoadPrivateKeyUrlEncode() throws Exception {
        try {
            String paramsStr = new String(Files.readAllBytes(Paths.get("./src/test/resources/authParams.json")));

            // load privatekey and encode it using url encoding
            ObjectMapper jsonMapper = ObjectMapperFactory.create();
            Map<String, String> authParamsMap = jsonMapper.readValue(paramsStr,
                    new TypeReference<HashMap<String, String>>() {
                    });
            String privateKeyContents = new String(Files.readAllBytes(Paths.get(authParamsMap.get("privateKey"))));
            authParamsMap.put("privateKey",
                    "data:application/x-pem-file," + encode(privateKeyContents).replace("+", "%20"));

            AuthenticationAthenz authEncode = new AuthenticationAthenz();
            authEncode.configure(jsonMapper.writeValueAsString(authParamsMap));

            PrivateKey privateKey = Crypto.loadPrivateKey(new File("./src/test/resources/tenant_private.pem"));
            Field field = authEncode.getClass().getDeclaredField("privateKey");
            field.setAccessible(true);
            PrivateKey key = (PrivateKey) field.get(authEncode);
            assertEquals(key, privateKey);
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testCopperArgos() throws Exception {
        @Cleanup
        AuthenticationAthenz caAuth = new AuthenticationAthenz();
        Field ztsClientField = caAuth.getClass().getDeclaredField("ztsClient");
        ztsClientField.setAccessible(true);
        ztsClientField.set(caAuth, new MockZTSClient("dummy"));

        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        Map<String, String> authParamsMap = new HashMap<>();
        authParamsMap.put("providerDomain", "test_provider");
        authParamsMap.put("ztsUrl", "https://localhost:4443/");

        try {
            caAuth.configure(jsonMapper.writeValueAsString(authParamsMap));
            fail("Should not succeed if some required parameters are missing");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }

        authParamsMap.put("x509CertChain", "data:application/x-pem-file;base64,aW52YWxpZAo=");
        try {
            caAuth.configure(jsonMapper.writeValueAsString(authParamsMap));
            fail("'data' scheme url should not be accepted");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }

        authParamsMap.put("x509CertChain", "file:./src/test/resources/copper_argos_client.crt");
        try {
            caAuth.configure(jsonMapper.writeValueAsString(authParamsMap));
            fail("Should not succeed if 'privateKey' or 'caCert' is missing");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }

        authParamsMap.put("privateKey", "./src/test/resources/copper_argos_client.key");
        authParamsMap.put("caCert", "./src/test/resources/copper_argos_ca.crt");
        caAuth.configure(jsonMapper.writeValueAsString(authParamsMap));

        Field x509CertChainPathField = caAuth.getClass().getDeclaredField("x509CertChainPath");
        x509CertChainPathField.setAccessible(true);
        String actualX509CertChainPath = (String) x509CertChainPathField.get(caAuth);
        assertFalse(actualX509CertChainPath.startsWith("file:"));
        assertFalse(actualX509CertChainPath.startsWith("./"));
        assertTrue(actualX509CertChainPath.endsWith("/src/test/resources/copper_argos_client.crt"));

        Field privateKeyPathField = caAuth.getClass().getDeclaredField("privateKeyPath");
        privateKeyPathField.setAccessible(true);
        String actualPrivateKeyPath = (String) privateKeyPathField.get(caAuth);
        assertFalse(actualPrivateKeyPath.startsWith("file:"));
        assertFalse(actualPrivateKeyPath.startsWith("./"));
        assertTrue(actualPrivateKeyPath.endsWith("/src/test/resources/copper_argos_client.key"));

        Field caCertPathField = caAuth.getClass().getDeclaredField("caCertPath");
        caCertPathField.setAccessible(true);
        String actualCaCertPath = (String) caCertPathField.get(caAuth);
        assertFalse(actualCaCertPath.startsWith("file:"));
        assertFalse(actualCaCertPath.startsWith("./"));
        assertTrue(actualCaCertPath.endsWith("/src/test/resources/copper_argos_ca.crt"));
    }

    @Test
    public void testAutoPrefetchEnabled() throws Exception {
        Field field = auth.getClass().getDeclaredField("autoPrefetchEnabled");
        field.setAccessible(true);
        assertFalse((boolean) field.get(auth));

        String paramsStr = new String(Files.readAllBytes(Paths.get("./src/test/resources/authParams.json")));
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        Map<String, String> authParamsMap = jsonMapper.readValue(paramsStr, new TypeReference<HashMap<String, String>>() { });

        authParamsMap.put("autoPrefetchEnabled", "true");
        AuthenticationAthenz auth1 = new AuthenticationAthenz();
        auth1.configure(jsonMapper.writeValueAsString(authParamsMap));
        assertTrue((boolean) field.get(auth1));
        auth1.close();

        authParamsMap.put("autoPrefetchEnabled", "false");
        AuthenticationAthenz auth2 = new AuthenticationAthenz();
        auth2.configure(jsonMapper.writeValueAsString(authParamsMap));
        assertFalse((boolean) field.get(auth2));
        auth2.close();
    }

    @Test
    public void testRoleHeaderSetting() throws Exception {
        assertEquals(auth.getAuthData().getHttpHeaders().iterator().next().getKey(), ZTSClient.getHeader());

        Field field = auth.getClass().getDeclaredField("ztsClient");
        field.setAccessible(true);

        String paramsStr = new String(Files.readAllBytes(Paths.get("./src/test/resources/authParams.json")));
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        Map<String, String> authParamsMap = jsonMapper.readValue(paramsStr, new TypeReference<HashMap<String, String>>() { });

        authParamsMap.put("roleHeader", "");
        AuthenticationAthenz auth1 = new AuthenticationAthenz();
        auth1.configure(jsonMapper.writeValueAsString(authParamsMap));
        field.set(auth1, new MockZTSClient("dummy"));
        assertEquals(auth1.getAuthData().getHttpHeaders().iterator().next().getKey(), ZTSClient.getHeader());
        auth1.close();

        authParamsMap.put("roleHeader", "Test-Role-Header");
        AuthenticationAthenz auth2 = new AuthenticationAthenz();
        auth2.configure(jsonMapper.writeValueAsString(authParamsMap));
        field.set(auth2, new MockZTSClient("dummy"));
        assertEquals(auth2.getAuthData().getHttpHeaders().iterator().next().getKey(), "Test-Role-Header");
        auth2.close();
    }

    @Test
    public void testZtsProxyUrlSetting() throws Exception {
        final String ztsProxyUrl = "https://example.com:4443/";
        final String paramsStr = new String(Files.readAllBytes(Paths.get("./src/test/resources/authParams.json")));
        final ObjectMapper jsonMapper = ObjectMapperFactory.create();
        final Map<String, String> authParamsMap = jsonMapper.readValue(paramsStr, new TypeReference<HashMap<String, String>>() { });

        try (MockedConstruction<ZTSClient> mockedZTSClient = Mockito.mockConstruction(ZTSClient.class, (mock, context) -> {
            final String actualZtsProxyUrl = (String) context.arguments().get(1);
            assertNull(actualZtsProxyUrl);

            when(mock.getRoleToken(any(), any(), anyInt(), anyInt(), anyBoolean())).thenReturn(mock(RoleToken.class));
        })) {
            authParamsMap.remove("ztsProxyUrl");
            final AuthenticationAthenz auth1 = new AuthenticationAthenz();
            auth1.configure(jsonMapper.writeValueAsString(authParamsMap));
            auth1.getAuthData();

            assertEquals(mockedZTSClient.constructed().size(), 1);

            auth1.close();

            authParamsMap.put("ztsProxyUrl", "");
            final AuthenticationAthenz auth2 = new AuthenticationAthenz();
            auth2.configure(jsonMapper.writeValueAsString(authParamsMap));
            auth2.getAuthData();

            assertEquals(mockedZTSClient.constructed().size(), 2);

            auth2.close();
        }

        try (MockedConstruction<ZTSClient> mockedZTSClient = Mockito.mockConstruction(ZTSClient.class, (mock, context) -> {
            final String actualZtsProxyUrl = (String) context.arguments().get(1);
            assertEquals(actualZtsProxyUrl, ztsProxyUrl);

            when(mock.getRoleToken(any(), any(), anyInt(), anyInt(), anyBoolean())).thenReturn(mock(RoleToken.class));
        })) {
            authParamsMap.put("ztsProxyUrl", ztsProxyUrl);
            final AuthenticationAthenz auth3 = new AuthenticationAthenz();
            auth3.configure(jsonMapper.writeValueAsString(authParamsMap));
            auth3.getAuthData();

            assertEquals(mockedZTSClient.constructed().size(), 1);

            auth3.close();
        }
    }
}
