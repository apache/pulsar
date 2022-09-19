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
import lombok.Builder;
import lombok.Getter;

/**
 * OAuth 2.0 Client Credentials flow data used to retrieve access token.
 */
@Builder
@Getter
public final class ClientCredentialsConfiguration {
    /**
     * Identity Provider's token issuer URL.
     */
    private final URL issuerUrl;

    /**
     * Optional. The audience identifier used by some Identity Providers, like Auth0.
     */
    private final String audience;

    /**
     * The {@link KeyFile} URL used to retrieve the client credentials. See {@link KeyFile} for file
     * format. This field was previously called the credentialsUrl.
     */
    private final URL keyFileUrl;

    /**
     * Optional. The value of the scope parameter is expressed as a list of space-delimited,
     * case-sensitive strings. The strings are defined by the authorization server.
     * If the value contains multiple space-delimited strings, their order does not matter,
     * and each string adds an additional access range to the requested scope.
     * Documented here: https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.2
     */
    private final String scope;
}
