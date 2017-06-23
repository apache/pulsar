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
package org.apache.pulsar.client.impl.auth;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationAthenz;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yahoo.athenz.zts.RoleToken;
import com.yahoo.athenz.zts.ZTSClient;

public class AuthenticationAthenzTest {

    private AuthenticationAthenz auth;
    private static final String TENANT_DOMAIN = "test_tenant";
    private static final String TENANT_SERVICE = "test_service";
    private static final String PROVIDER_DOMAIN = "test_provider";
    private static final String PRIVATE_KEY_PATH = "./src/test/resources/tenant_private.pem";

    @BeforeClass
    public void setup() throws Exception {

        // Configure parameters
        Map<String, String> params = new HashMap<String, String>() {
            {
                put("tenantDomain", TENANT_DOMAIN);
                put("tenantService", TENANT_SERVICE);
                put("providerDomain", PROVIDER_DOMAIN);
                put("privateKeyPath", PRIVATE_KEY_PATH);
            }
        };
        auth = new AuthenticationAthenz();
        auth.configure(params);

        // Set mock ztsClient which returns fixed token instead of fetching from ZTS server
        Field field = auth.getClass().getDeclaredField("ztsClient");
        field.setAccessible(true);
        ZTSClient mockZtsClient = new ZTSClient("dummy") {
            @Override
            public RoleToken getRoleToken(String domainName, String roleName, Integer minExpiryTime,
                    Integer maxExpiryTime, boolean ignoreCache) {

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

        };
        field.set(auth, mockZtsClient);
    }

    @Test
    public void testGetAuthData() throws Exception {

        com.yahoo.athenz.auth.token.RoleToken roleToken = new com.yahoo.athenz.auth.token.RoleToken(
                auth.getAuthData().getCommandData());
        assertEquals(roleToken.getPrincipal(), String.format("%s.%s", TENANT_DOMAIN, TENANT_SERVICE));

        int count = 0;
        for (Map.Entry<String, String> header : auth.getAuthData().getHttpHeaders()) {
            if (header.getKey() == ZTSClient.getHeader()) {
                com.yahoo.athenz.auth.token.RoleToken roleTokenFromHeader = new com.yahoo.athenz.auth.token.RoleToken(
                        header.getValue());
                assertEquals(roleTokenFromHeader.getPrincipal(), String.format("%s.%s", TENANT_DOMAIN, TENANT_SERVICE));
                count++;
            }
        }
        assertEquals(count, 1);
    }
}