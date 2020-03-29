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
import javax.naming.AuthenticationException;

import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.AuthData;

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
     * This method is useful in one stage authn, if you're not doing one stage or if you're providing
     * your own state implementation for one stage authn, it should throw an exception.
     *
     * @param authData
     *            provider specific authentication data
     * @return the "role" string for the authenticated connection, if the authentication was successful
     * @throws AuthenticationException
     *             if the credentials are not valid
     */
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
     * Set response, according to passed in request.
     * and return whether we should do following chain.doFilter or not.
     */
    default boolean authenticateHttpRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        throw new AuthenticationException("Not supported");
    }
}
