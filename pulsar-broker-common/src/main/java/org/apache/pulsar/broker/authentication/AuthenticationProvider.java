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

import java.io.Closeable;
import java.io.IOException;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import javax.naming.AuthenticationException;

import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Provider of authentication mechanism
 */
public interface AuthenticationProvider extends Closeable {

    /**
     * Perform initialization for the authentication provider
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
     * Validate the authentication for the given credentials with the specified authentication data.
     *
     * <p>Warning: the calling thread is an IO thread. Any implementations that rely on blocking behavior
     * must ensure that the execution is completed on using a separate thread pool to ensure IO threads
     * are never blocked.</p>
     *
     * <p>Note: this method is marked as unstable because the Pulsar code base only calls it for the
     * Pulsar Broker Auth SASL plugin. All non SASL HTTP requests are authenticated using the
     * {@link AuthenticationProvider#authenticateAsync(AuthenticationDataSource)} method. As such,
     * this method might be removed in favor of the SASL provider implementing the
     * {@link AuthenticationProvider#authenticateAsync(AuthenticationDataSource)} method.</p>
     *
     * @return Set response, according to passed in request.
     * and return whether we should do following chain.doFilter or not.
     */
    @InterfaceStability.Unstable
    default CompletableFuture<Boolean> authenticateHttpRequestAsync(HttpServletRequest request,
                                                                    HttpServletResponse response) {
        try {
            return CompletableFuture.completedFuture(this.authenticateHttpRequest(request, response));
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
    }

    /**
     * Set response, according to passed in request.
     * and return whether we should do following chain.doFilter or not.
     * @deprecated use and implement {@link AuthenticationProvider#authenticateHttpRequestAsync} instead.
     */
    @Deprecated
    default boolean authenticateHttpRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        throw new AuthenticationException("Not supported");
    }
}
