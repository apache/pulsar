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

import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;

import javax.naming.AuthenticationException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Performs multistage authentication by extending the paradigm created in {@link MockAuthenticationProvider}.
 */
public class MockMultiStageAuthenticationState implements AuthenticationState {

    private final MockMultiStageAuthenticationProvider provider;
    private String authRole = null;

    MockMultiStageAuthenticationState(MockMultiStageAuthenticationProvider provider) {
        this.provider = provider;
    }

    @Override
    public String getAuthRole() throws AuthenticationException {
        if (authRole == null) {
            throw new AuthenticationException("Must authenticate first");
        }
        return null;
    }

    @Override
    public AuthData authenticate(AuthData authData) throws AuthenticationException {
        String data = new String(authData.getBytes(), UTF_8);
        String[] parts = data.split("\\.");
        if (parts.length == 2) {
            if ("challenge".equals(parts[0])) {
                return AuthData.of("challenged".getBytes());
            } else {
                AuthenticationDataCommand command = new AuthenticationDataCommand(data);
                authRole = provider.authenticate(command);
                // Auth successful, no more auth required
                return null;
            }
        }
        throw new AuthenticationException("Failed to authenticate");
    }

    @Override
    public AuthenticationDataSource getAuthDataSource() {
        return null;
    }

    @Override
    public boolean isComplete() {
        return authRole != null;
    }
}
