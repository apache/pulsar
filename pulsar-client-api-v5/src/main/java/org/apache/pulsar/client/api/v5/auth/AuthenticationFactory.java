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
package org.apache.pulsar.client.api.v5.auth;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.internal.PulsarClientProvider;

/**
 * Factory for creating common authentication providers.
 */
public final class AuthenticationFactory {

    private AuthenticationFactory() {
    }

    /**
     * Create token-based authentication with a static token.
     *
     * @param token the JWT or other authentication token string
     * @return an {@link Authentication} instance configured with the given token
     */
    public static Authentication token(String token) {
        return PulsarClientProvider.get().authenticationToken(token);
    }

    /**
     * Create token-based authentication with a dynamic token supplier.
     *
     * <p>The supplier is invoked each time the client needs to authenticate,
     * allowing for token refresh without recreating the client.
     *
     * @param tokenSupplier a supplier that provides the current authentication token
     * @return an {@link Authentication} instance that retrieves tokens from the supplier
     */
    public static Authentication token(Supplier<String> tokenSupplier) {
        return PulsarClientProvider.get().authenticationToken(tokenSupplier);
    }

    /**
     * Create the built-in TLS mutual-authentication marker plugin (PIP-478).
     *
     * <p>This plugin carries no TLS material of its own: it only advertises the {@code "tls"}
     * authentication method so the broker authenticates from the client certificate presented during the
     * TLS handshake. Configure the client certificate, private key, and trust material through the client
     * builder's {@code tlsPolicy(...)} (e.g. {@code TlsPolicy.pem(trustCerts, certFile, keyFile)}); the
     * transport reads its material from the client TLS factory.
     *
     * @return an {@link Authentication} instance that advertises the {@code "tls"} method
     */
    public static Authentication tls() {
        return PulsarClientProvider.get().authenticationTls();
    }

    /**
     * Create an authentication provider by plugin class name and parameter string.
     *
     * @param authPluginClassName the fully qualified class name of the authentication plugin
     * @param authParamsString    the authentication parameters as a serialized string
     * @return an {@link Authentication} instance created from the specified plugin
     * @throws PulsarClientException if the plugin class cannot be loaded or instantiated
     */
    public static Authentication create(String authPluginClassName, String authParamsString)
            throws PulsarClientException {
        return PulsarClientProvider.get().createAuthentication(authPluginClassName, authParamsString);
    }

    /**
     * Create an authentication provider by plugin class name and parameter map.
     *
     * @param authPluginClassName the fully qualified class name of the authentication plugin
     * @param authParams          the authentication parameters as key-value pairs
     * @return an {@link Authentication} instance created from the specified plugin
     * @throws PulsarClientException if the plugin class cannot be loaded or instantiated
     */
    public static Authentication create(String authPluginClassName, Map<String, String> authParams)
            throws PulsarClientException {
        return PulsarClientProvider.get().createAuthentication(authPluginClassName, authParams);
    }
}
