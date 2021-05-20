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

import com.google.common.collect.Lists;
import java.net.SocketAddress;
import java.util.List;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import org.apache.pulsar.common.api.AuthData;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.broker.authentication.AuthenticationProvider.MULTI_ROLE_NOT_SUPPORTED;

/**
 * Interface for authentication state.
 *
 * It tell broker whether the authentication is completed or not,
 * if completed, what is the AuthRole is.
 */
public class OneStageAuthenticationState implements AuthenticationState {

    private final AuthenticationDataSource authenticationDataSource;
    private List<String> authRoles;

    public OneStageAuthenticationState(AuthData authData,
                                       SocketAddress remoteAddress,
                                       SSLSession sslSession,
                                       AuthenticationProvider provider) throws AuthenticationException {
        this.authenticationDataSource = new AuthenticationDataCommand(
            new String(authData.getBytes(), UTF_8), remoteAddress, sslSession);
        try {
            this.authRoles = provider.authenticate(authenticationDataSource, true);
        } catch (AuthenticationException e) {
            if (e.getMessage().equals(MULTI_ROLE_NOT_SUPPORTED)) {
                this.authRoles = Lists.newArrayList(provider.authenticate(authenticationDataSource));
            } else {
                throw e;
            }
        }
    }

    @Override
    public String getAuthRole() {
        return authRoles.get(0);
    }

    @Override
    public List<String> getAuthRoles() {
        return authRoles;
    }

    @Override
    public AuthenticationDataSource getAuthDataSource() {
        return authenticationDataSource;
    }

    @Override
    public AuthData authenticate(AuthData authData) {
        return null;
    }

    @Override
    public boolean isComplete() {
        return true;
    }
}
