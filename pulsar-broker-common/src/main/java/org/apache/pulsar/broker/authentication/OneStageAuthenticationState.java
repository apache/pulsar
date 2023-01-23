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
package org.apache.pulsar.broker.authentication;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import org.apache.pulsar.common.api.AuthData;

/**
 * A class to track single stage authentication. This class assumes that:
 * 1. {@link #authenticateAsync(AuthData)} is called once and when the {@link CompletableFuture} completes,
 *    authentication is complete.
 * 2. Authentication does not expire, so {@link #isExpired()} always returns false.
 * <p>
 * See {@link AuthenticationState} for Pulsar's contract on how this interface is used by Pulsar.
 */
public class OneStageAuthenticationState implements AuthenticationState {

    private AuthenticationDataSource authenticationDataSource;
    private final SocketAddress remoteAddress;
    private final SSLSession sslSession;
    private final AuthenticationProvider provider;
    private volatile String authRole;


    /**
     * Constructor for a {@link OneStageAuthenticationState} where there is no authentication performed during
     * initialization.
     * @param remoteAddress - remoteAddress associated with the {@link AuthenticationState}
     * @param sslSession - sslSession associated with the {@link AuthenticationState}
     * @param provider - {@link AuthenticationProvider} to use to verify {@link AuthData}
     */
    public OneStageAuthenticationState(AuthData authData,
                                       SocketAddress remoteAddress,
                                       SSLSession sslSession,
                                       AuthenticationProvider provider) {
        this.provider = provider;
        this.remoteAddress = remoteAddress;
        this.sslSession = sslSession;
    }

    public OneStageAuthenticationState(HttpServletRequest request, AuthenticationProvider provider) {
        // Must initialize this here for backwards compatibility with http authentication
        this.authenticationDataSource = new AuthenticationDataHttps(request);
        this.provider = provider;
        // These are not used when invoking this constructor.
        this.remoteAddress = null;
        this.sslSession = null;
    }

    @Override
    public String getAuthRole() throws AuthenticationException {
        if (authRole == null) {
            throw new AuthenticationException("Must authenticate before calling getAuthRole");
        }
        return authRole;
    }

    @Override
    public AuthenticationDataSource getAuthDataSource() {
        return authenticationDataSource;
    }

    /**
     * Warning: this method is not intended to be called concurrently.
     */
    @Override
    public CompletableFuture<AuthData> authenticateAsync(AuthData authData) {
        if (authRole != null) {
            // Authentication is already completed
            return CompletableFuture.completedFuture(null);
        }
        this.authenticationDataSource = new AuthenticationDataCommand(
                new String(authData.getBytes(), UTF_8), remoteAddress, sslSession);

        return provider
                .authenticateAsync(authenticationDataSource)
                .thenApply(role -> {
                    this.authRole = role;
                    // Single stage authentication always returns null
                    return null;
                });
    }

    /**
     * @deprecated use {@link #authenticateAsync(AuthData)}
     */
    @Deprecated(since = "2.12.0")
    @Override
    public AuthData authenticate(AuthData authData) throws AuthenticationException {
        try {
            return authenticateAsync(authData).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @deprecated rely on result from {@link #authenticateAsync(AuthData)}. For more information, see the Javadoc
     * for {@link AuthenticationState#isComplete()}.
     */
    @Deprecated(since = "2.12.0")
    @Override
    public boolean isComplete() {
        return authRole != null;
    }
}
