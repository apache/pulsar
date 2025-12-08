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
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;
import org.testng.annotations.Test;

public class AuthenticationOAuth2StandardAuthzServerTest {

    @Test
    public void testConfigureWithOAuth2MetadataPath() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("type", "client_credentials");
        params.put("privateKey", "data:base64,e30=");
        params.put("issuerUrl", "http://localhost");
        params.put("audience", "test-audience");
        ObjectMapper mapper = new ObjectMapper();
        String authParams = mapper.writeValueAsString(params);
        AuthenticationOAuth2StandardAuthzServer auth = new AuthenticationOAuth2StandardAuthzServer();
        auth.configure(authParams);
        assertTrue(auth.flow instanceof ClientCredentialsFlow);
        ClientCredentialsFlow flow = (ClientCredentialsFlow) auth.flow;
        Field wellKnownMetadataPathField = FlowBase.class.getDeclaredField("wellKnownMetadataPath");
        wellKnownMetadataPathField.setAccessible(true);
        String wellKnownMetadataPath = (String) wellKnownMetadataPathField.get(flow);
        assertEquals(wellKnownMetadataPath, DefaultMetadataResolver.getOAuthWellKnownMetadataPath());
    }
}
