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

import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.net.URL;
import org.apache.pulsar.client.api.Authentication;
import org.testng.annotations.Test;

public class AuthenticationFactoryOAuth2Test {

    @Test
    public void testClientCredentials() throws IOException {
        URL issuerUrl = new URL("http://localhost");
        URL credentialsUrl = new URL("http://localhost");
        String audience = "audience";
        try (Authentication authentication =
                     AuthenticationFactoryOAuth2.clientCredentials(issuerUrl, credentialsUrl, audience)) {
            assertTrue(authentication instanceof AuthenticationOAuth2);
        }
    }

}
