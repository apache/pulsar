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
package org.apache.pulsar.client.impl.auth.httpclient;


import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for HTTP clients used in authentication providers.
 *
 * <p>This class encapsulates the configuration parameters needed to create HTTP clients
 * for authentication-related HTTP requests, such as OAuth2 token exchange requests.
 *
 * <h2>Configuration Parameters</h2>
 * <ul>
 *   <li><strong>readTimeout</strong>: Maximum time to wait for a response from the server
 *       (default: 30000 ms)
 *   <li><strong>connectTimeout</strong>: Maximum time to establish a connection
 *       (default: 10000 ms)
 *   <li><strong>trustCertsFilePath</strong>: Path to a custom trust certificate file
 *       for TLS/SSL connections
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Using the builder pattern
 * AuthenticationHttpClientConfig config = AuthenticationHttpClientConfig.builder()
 *     .readTimeout(30000)
 *     .connectTimeout(10000)
 *     .trustCertsFilePath("/path/to/certs.pem")
 *     .build();
 *
 * // Using the factory
 * AuthenticationHttpClientFactory factory = new AuthenticationHttpClientFactory(config, context);
 * AsyncHttpClient httpClient = factory.createHttpClient();
 * }</pre>
 *
 * <p>This configuration is primarily used by {@link AuthenticationHttpClientFactory} to
 * create properly configured HTTP clients for authentication providers.
 *
 * @see AuthenticationHttpClientFactory
 * @see org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2
 */
@Getter
public class AuthenticationHttpClientConfig {
    private int readTimeout = 30000;
    private int connectTimeout = 10000;
    private final String trustCertsFilePath;

    @Builder(builderClassName = "ConfigBuilder")
    public AuthenticationHttpClientConfig(int readTimeout, int connectTimeout, String trustCertsFilePath) {
        this.readTimeout = readTimeout > 0 ? readTimeout : this.readTimeout;
        this.connectTimeout = connectTimeout > 0 ? connectTimeout : this.connectTimeout;
        this.trustCertsFilePath = trustCertsFilePath;
    }
}
