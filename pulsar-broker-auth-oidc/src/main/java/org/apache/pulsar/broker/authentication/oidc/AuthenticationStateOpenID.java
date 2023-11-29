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
package org.apache.pulsar.broker.authentication.oidc;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;

/**
 * Class representing the authentication state of a single connection.
 */
class AuthenticationStateOpenID implements AuthenticationState {
    private final AuthenticationProviderOpenID provider;
    private AuthenticationDataSource authenticationDataSource;
    private volatile String role;
    private final SocketAddress remoteAddress;
    private final SSLSession sslSession;
    private volatile long expiration;

    AuthenticationStateOpenID(
            AuthenticationProviderOpenID provider,
            SocketAddress remoteAddress,
            SSLSession sslSession) {
        this.provider = provider;
        this.remoteAddress = remoteAddress;
        this.sslSession = sslSession;
    }

    @Override
    public String getAuthRole() throws AuthenticationException {
        if (role == null) {
            throw new AuthenticationException("Authentication has not completed");
        }
        return role;
    }

    @Deprecated
    @Override
    public AuthData authenticate(AuthData authData) throws AuthenticationException {
        // This method is not expected to be called and is subject to removal.
        throw new AuthenticationException("Not supported");
    }

    @Override
    public CompletableFuture<AuthData> authenticateAsync(AuthData authData) {
        final String token = new String(authData.getBytes(), UTF_8);
        this.authenticationDataSource = new AuthenticationDataCommand(token, remoteAddress, sslSession);
        return provider
                .authenticateTokenAsync(authenticationDataSource)
                .thenApply(jwt -> {
                    this.role = provider.getRole(jwt);
                    // OIDC requires setting the exp claim, so this should never be null.
                    // We verify it is not null during token validation.
                    this.expiration = jwt.getExpiresAt().getTime();
                    // Single stage authentication, so return null here
                    return null;
                });
    }

    @Override
    public AuthenticationDataSource getAuthDataSource() {
        return authenticationDataSource;
    }

    @Override
    public boolean isComplete() {
        return role != null;
    }

    @Override
    public boolean isExpired() {
        return System.currentTimeMillis() > expiration;
    }
}
