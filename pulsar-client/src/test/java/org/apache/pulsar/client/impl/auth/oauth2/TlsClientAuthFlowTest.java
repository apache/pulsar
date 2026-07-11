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
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class TlsClientAuthFlowTest {

    @Test
    public void testFromParametersWithoutClientId() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("tlsCertFile", "/path/to/cert.pem");
        params.put("tlsKeyFile", "/path/to/key.pem");
        params.put("issuerUrl", "http://localhost");
        params.put("scope", "http://localhost");
        OAuth2MockHttpClient.withMockedSslFactory(() -> {
            TlsClientAuthFlow flow = TlsClientAuthFlow.fromParameters(params);
            assertEquals(flow.getClientId(), "pulsar-client");
            flow.close();
        });
    }
}
