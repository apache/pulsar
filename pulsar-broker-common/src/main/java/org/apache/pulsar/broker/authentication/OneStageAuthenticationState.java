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
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import org.apache.pulsar.common.api.AuthData;

/**
 * A class to track single stage authentication. This class assumes that:
 * 1. {@link #authenticate(AuthData)} is called once and then authentication is completed
 * 2. Authentication does not expire, so {@link #isExpired()} always returns false.
 * <p>
 * See {@link AuthenticationState} for Pulsar's contract on how this interface is used by Pulsar.
 */
public class OneStageAuthenticationState implements AuthenticationState {

    private AuthenticationDataSource authenticationDataSource;
    private final SocketAddress remoteAddress;
    private final SSLSession sslSession;
    private final AuthenticationProvider provider;
    private String authRole;

    /**
     * Constructor for a {@link OneStageAuthenticationState} where there is no authentication performed on
     * initialization.
     * @param remoteAddress - remoteAddress associated with the {@link AuthenticationState}
     * @param sslSession - sslSession associated with the {@link AuthenticationState}
     * @param provider - {@link AuthenticationProvider} to use to verify {@link AuthData}
     */
    public OneStageAuthenticationState(SocketAddress remoteAddress,
                                       SSLSession sslSession,
                                       AuthenticationProvider provider) {
        this.provider = provider;
        this.remoteAddress = remoteAddress;
        this.sslSession = sslSession;
    }

    /**
     * @deprecated use OneStageAuthenticationState constructor without {@link AuthData}. In order to maintain some
     * backwards compatibility, this constructor validates the parameterized {@link AuthData}.
     */
    @Deprecated(since = "2.12.0")
    public OneStageAuthenticationState(AuthData authData,
                                       SocketAddress remoteAddress,
                                       SSLSession sslSession,
                                       AuthenticationProvider provider) throws AuthenticationException {
        this.authenticationDataSource = new AuthenticationDataCommand(
            new String(authData.getBytes(), UTF_8), remoteAddress, sslSession);
        this.authRole = provider.authenticate(authenticationDataSource);
        this.provider = provider;
        this.remoteAddress = remoteAddress;
        this.sslSession = sslSession;
    }

    /**
     * @deprecated method was only ever used for
     * {@link AuthenticationProvider#newHttpAuthState(HttpServletRequest)}. That state object wasn't used other than
     * to retrieve the {@link AuthenticationDataSource}, so this constructor is deprecated now. In order to maintain
     * some backwards compatibility, this constructor validates the parameterized {@link AuthData}.
     */
    @Deprecated(since = "2.12.0")
    public OneStageAuthenticationState(HttpServletRequest request, AuthenticationProvider provider)
            throws AuthenticationException {
        this.authenticationDataSource = new AuthenticationDataHttps(request);
        this.authRole = provider.authenticate(authenticationDataSource);
        // These are not used when this constructor is invoked, set them to null.
        this.provider = null;
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

    @Override
    public AuthData authenticate(AuthData authData) throws AuthenticationException {
        this.authenticationDataSource = new AuthenticationDataCommand(
                new String(authData.getBytes(), UTF_8), remoteAddress, sslSession);
        this.authRole = provider.authenticate(authenticationDataSource);
        return null;
    }

    @Override
    public boolean isComplete() {
        return authRole != null;
    }
}
