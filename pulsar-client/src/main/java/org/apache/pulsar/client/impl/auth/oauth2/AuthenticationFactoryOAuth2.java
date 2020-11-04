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

import java.net.URL;
import java.time.Clock;
import org.apache.pulsar.client.api.Authentication;

/**
 * Factory class that allows to create {@link Authentication} instances
 * for OAuth 2.0 authentication methods.
 */
public final class AuthenticationFactoryOAuth2 {

    /**
     * Authenticate with client credentials.
     *
     * @param issuerUrl the issuer URL
     * @param credentialsUrl the credentials URL
     * @param audience the audience identifier
     * @return an Authentication object
     */
    public static Authentication clientCredentials(URL issuerUrl, URL credentialsUrl, String audience) {
        ClientCredentialsFlow flow = ClientCredentialsFlow.builder()
                .issuerUrl(issuerUrl)
                .privateKey(credentialsUrl.toExternalForm())
                .audience(audience)
                .build();
        return new AuthenticationOAuth2(flow, Clock.systemDefaultZone());
    }
}
