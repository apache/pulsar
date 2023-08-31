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

import static org.apache.pulsar.broker.web.AuthenticationFilter.AuthenticatedDataAttributeName;
import static org.apache.pulsar.broker.web.AuthenticationFilter.AuthenticatedRoleAttributeName;
import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Provider of authentication mechanism.
 */
public interface AuthenticationProvider extends Closeable {

    /**
     * Perform initialization for the authentication provider.
     *
     * @param config
     *            broker config object
     * @throws IOException
     *             if the initialization fails
     */
    void initialize(ServiceConfiguration config) throws IOException;

    /**
     * @return the authentication method name supported by this provider
     */
    String getAuthMethodName();

    /**
     * Validate the authentication for the given credentials with the specified authentication data.
     * This method is useful in one stage authentication, if you're not doing one stage or if you're providing
     * your own state implementation for one stage authentication, it should return a failed future.
     *
     * <p>Warning: the calling thread is an IO thread. Any implementation that relies on blocking behavior
     * must ensure that the execution is completed using a separate thread pool to ensure IO threads
     * are never blocked.</p>
     *
     * @param authData authentication data generated while initiating a connection. There are several types,
     *                 including, but not strictly limited to, {@link AuthenticationDataHttp},
     *                 {@link AuthenticationDataHttps}, and {@link AuthenticationDataCommand}.
     * @return A completed future with the "role" string for the authenticated connection, if authentication is
     * successful, or a failed future if the authData is not valid.
     */
    default CompletableFuture<String> authenticateAsync(AuthenticationDataSource authData) {
        try {
            return CompletableFuture.completedFuture(this.authenticate(authData));
        } catch (AuthenticationException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    /**
     * Validate the authentication for the given credentials with the specified authentication data.
     * This method is useful in one stage authn, if you're not doing one stage or if you're providing
     * your own state implementation for one stage authn, it should throw an exception.
     *
     * @param authData
     *            provider specific authentication data
     * @return the "role" string for the authenticated connection, if the authentication was successful
     * @throws AuthenticationException
     *             if the credentials are not valid
     * @deprecated use and implement {@link AuthenticationProvider#authenticateAsync(AuthenticationDataSource)} instead.
     */
    @Deprecated
    default String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        throw new AuthenticationException("Not supported");
    }

    /**
     * Create an authentication data State use passed in AuthenticationDataSource.
     */
    default AuthenticationState newAuthState(AuthData authData,
                                             SocketAddress remoteAddress,
                                             SSLSession sslSession)
        throws AuthenticationException {
        return new OneStageAuthenticationState(authData, remoteAddress, sslSession, this);
    }

    /**
     * Create an http authentication data State use passed in AuthenticationDataSource.
     * @deprecated implementations that previously relied on this should update their implementation of
     * {@link #authenticateHttpRequest(HttpServletRequest, HttpServletResponse)} or of
     * {@link #authenticateHttpRequestAsync(HttpServletRequest, HttpServletResponse)} so that the desired attributes
     * are added in those methods.
     *
     * <p>Note: this method was only ever used to generate an {@link AuthenticationState} object in order to generate
     * an {@link AuthenticationDataSource} that was added as the {@link AuthenticatedDataAttributeName} attribute to
     * the http request. Removing this method removes an unnecessary step in the authentication flow.</p>
     */
    @Deprecated(since = "3.0.0")
    default AuthenticationState newHttpAuthState(HttpServletRequest request)
            throws AuthenticationException {
        return new OneStageAuthenticationState(request, this);
    }

    /**
     * Validate the authentication for the given credentials with the specified authentication data.
     *
     * <p>Implementations of this method MUST modify the request by adding the {@link AuthenticatedRoleAttributeName}
     * and the {@link AuthenticatedDataAttributeName} attributes.</p>
     *
     * <p>Warning: the calling thread is an IO thread. Any implementations that rely on blocking behavior
     * must ensure that the execution is completed on using a separate thread pool to ensure IO threads
     * are never blocked.</p>
     *
     * @return Set response, according to passed in request, and return whether we should do following chain.doFilter.
     * @throws Exception when authentication failed
     * and return whether we should do following chain.doFilter or not.
     */
    default CompletableFuture<Boolean> authenticateHttpRequestAsync(HttpServletRequest request,
                                                                    HttpServletResponse response) {
        try {
            return CompletableFuture.completedFuture(this.authenticateHttpRequest(request, response));
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
    }

    default void incrementFailureMetric(Enum<?> errorCode) {
        AuthenticationMetrics.authenticateFailure(getClass().getSimpleName(), getAuthMethodName(), errorCode);
    }

    /**
     * Set response, according to passed in request.
     * and return whether we should do following chain.doFilter or not.
     *
     * <p>Implementations of this method MUST modify the request by adding the {@link AuthenticatedRoleAttributeName}
     * and the {@link AuthenticatedDataAttributeName} attributes.</p>
     *
     * @return Set response, according to passed in request, and return whether we should do following chain.doFilter.
     * @throws Exception when authentication failed
     * @deprecated use and implement {@link AuthenticationProvider#authenticateHttpRequestAsync} instead.
     */
    @Deprecated
    default boolean authenticateHttpRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        try {
            AuthenticationState authenticationState = newHttpAuthState(request);
            String role = authenticateAsync(authenticationState.getAuthDataSource()).get();
            request.setAttribute(AuthenticatedRoleAttributeName, role);
            request.setAttribute(AuthenticatedDataAttributeName, authenticationState.getAuthDataSource());
            return true;
        } catch (AuthenticationException e) {
            throw e;
        } catch (Exception e) {
            if (e instanceof ExecutionException && e.getCause() instanceof AuthenticationException) {
                throw (AuthenticationException) e.getCause();
            } else {
                throw new AuthenticationException("Failed to authentication http request");
            }
        }
    }
}
