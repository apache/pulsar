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
package org.apache.pulsar.broker.authentication;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.AuthData;

/**
 * An authentication provider wraps a list of auth providers.
 */
@Slf4j
public class AuthenticationProviderList implements AuthenticationProvider {

    private interface AuthProcessor<T, P> {

        T apply(P process) throws AuthenticationException;

    }

    static <T, P> T applyAuthProcessor(List<P> processors, AuthProcessor<T, P> authFunc)
        throws AuthenticationException {
        AuthenticationException authenticationException = null;
        for (P ap : processors) {
            try {
                return authFunc.apply(ap);
            } catch (AuthenticationException ae) {
                if (log.isDebugEnabled()) {
                    log.debug("Authentication failed for auth provider " + ap.getClass() + ": ", ae);
                }
                // Store the exception so we can throw it later instead of a generic one
                authenticationException = ae;
            }
        }

        if (null == authenticationException) {
            throw new AuthenticationException("Authentication required");
        } else {
            throw authenticationException;
        }

    }

    private static class AuthenticationListState implements AuthenticationState {

        private final List<AuthenticationState> states;
        private AuthenticationState authState;

        AuthenticationListState(List<AuthenticationState> states) {
            this.states = states;
            this.authState = states.get(0);
        }

        private AuthenticationState getAuthState() throws AuthenticationException {
            if (authState != null) {
                return authState;
            } else {
                throw new AuthenticationException("Authentication state is not initialized");
            }
        }

        @Override
        public String getAuthRole() throws AuthenticationException {
            return getAuthState().getAuthRole();
        }

        @Override
        public AuthData authenticate(AuthData authData) throws AuthenticationException {
            return applyAuthProcessor(
                states,
                as -> {
                    AuthData ad = as.authenticate(authData);
                    AuthenticationListState.this.authState = as;
                    return ad;
                }
            );
        }

        @Override
        public AuthenticationDataSource getAuthDataSource() {
            return authState.getAuthDataSource();
        }

        @Override
        public boolean isComplete() {
            return authState.isComplete();
        }

        @Override
        public long getStateId() {
            if (null != authState) {
                return authState.getStateId();
            } else {
                return states.get(0).getStateId();
            }
        }

        @Override
        public boolean isExpired() {
            return authState.isExpired();
        }

        @Override
        public AuthData refreshAuthentication() throws AuthenticationException {
            return getAuthState().refreshAuthentication();
        }
    }

    private final List<AuthenticationProvider> providers;

    public AuthenticationProviderList(List<AuthenticationProvider> providers) {
        this.providers = providers;
    }

    public List<AuthenticationProvider> getProviders() {
        return providers;
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        for (AuthenticationProvider ap : providers) {
            ap.initialize(config);
        }
    }

    @Override
    public String getAuthMethodName() {
        return providers.get(0).getAuthMethodName();
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        return applyAuthProcessor(
            providers,
            provider -> provider.authenticate(authData)
        );
    }

    @Override
    public AuthenticationState newAuthState(AuthData authData, SocketAddress remoteAddress, SSLSession sslSession)
        throws AuthenticationException {
        final List<AuthenticationState> states = new ArrayList<>(providers.size());

        AuthenticationException authenticationException = null;
        try {
            applyAuthProcessor(
                providers,
                provider -> {
                    AuthenticationState state = provider.newAuthState(authData, remoteAddress, sslSession);
                    states.add(state);
                    return state;
                }
            );
        } catch (AuthenticationException ae) {
            authenticationException = ae;
        }
        if (states.isEmpty()) {
            log.debug("Failed to initialize a new auth state from {}", remoteAddress, authenticationException);
            if (authenticationException != null) {
                throw authenticationException;
            } else {
                throw new AuthenticationException("Failed to initialize a new auth state from " + remoteAddress);
            }
        } else {
            return new AuthenticationListState(states);
        }
    }

    @Override
    public boolean authenticateHttpRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        Boolean authenticated = applyAuthProcessor(
            providers,
            provider -> {
                try {
                    return provider.authenticateHttpRequest(request, response);
                } catch (Exception e) {
                    if (e instanceof AuthenticationException) {
                        throw (AuthenticationException) e;
                    } else {
                        throw new AuthenticationException("Failed to authentication http request");
                    }
                }
            }
        );
        return authenticated;
    }

    @Override
    public void close() throws IOException {
        for (AuthenticationProvider provider : providers) {
            provider.close();
        }
    }
}
