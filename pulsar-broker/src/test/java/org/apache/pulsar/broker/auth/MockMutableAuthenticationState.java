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
package org.apache.pulsar.broker.auth;

import static java.nio.charset.StandardCharsets.UTF_8;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;

// MockMutableAuthenticationState always update the authentication data source and auth role.
public class MockMutableAuthenticationState implements AuthenticationState {

    final AuthenticationProvider provider;
    AuthenticationDataSource authenticationDataSource;
    volatile String authRole;

    MockMutableAuthenticationState(AuthenticationProvider provider) {
        this.provider = provider;
    }

    @Override
    public String getAuthRole() throws AuthenticationException {
        if (authRole == null) {
            throw new AuthenticationException("Must authenticate first.");
        }
        return authRole;
    }

    @Override
    public AuthData authenticate(AuthData authData) throws AuthenticationException {
        authenticationDataSource = new AuthenticationDataCommand(new String(authData.getBytes(), UTF_8));
        authRole = provider.authenticate(authenticationDataSource);
        return null;
    }

    @Override
    public AuthenticationDataSource getAuthDataSource() {
        return authenticationDataSource;
    }

    @Override
    public boolean isComplete() {
        return authRole != null;
    }

    @Override
    public boolean isExpired() {
        return false;
    }

}
