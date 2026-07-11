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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationData;
import org.testng.annotations.Test;

/**
 * Verifies that {@code AuthenticationAdapter}'s V5 wrapper bridges through to the
 * v4 {@code AuthenticationDataProvider} on every dimension exposed by the V5
 * {@link AuthenticationData} surface — previously {@code authData()} was a TODO
 * stub returning {@code null}, which silently broke any V5 caller that asked the
 * wrapped auth for credentials.
 */
public class AuthenticationAdapterTest {

    @Test
    public void testTokenAuthExposesCommandData() throws Exception {
        Authentication v5Auth = AuthenticationAdapter.token("my-jwt");

        AuthenticationData data = v5Auth.authData();
        assertNotNull(data, "authData() must not return null after the wiring fix");
        assertTrue(data.hasDataFromCommand(),
                "token auth should expose its credential via the binary-protocol command path");
        assertEquals(data.getCommandData(), "my-jwt");
    }

    @Test
    public void testAuthMethodNamePassesThrough() throws Exception {
        Authentication v5Auth = AuthenticationAdapter.token("any-token");
        assertEquals(v5Auth.authMethodName(), "token");
    }

    @Test
    public void testAuthDataPerBrokerHostDelegates() throws Exception {
        Authentication v5Auth = AuthenticationAdapter.token("per-host-token");

        AuthenticationData data = v5Auth.authData("broker-1.example.com");
        assertNotNull(data, "authData(brokerHost) must not return null");
        assertEquals(data.getCommandData(), "per-host-token");
    }
}
