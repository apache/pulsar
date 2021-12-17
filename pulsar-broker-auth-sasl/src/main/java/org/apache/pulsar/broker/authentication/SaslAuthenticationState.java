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

import java.util.concurrent.atomic.AtomicLong;

import javax.naming.AuthenticationException;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.AuthData;

/**
 * Interface for authentication state.
 *
 * It is basically holding the the authentication state.
 * It tell broker whether the authentication is completed or not,
 */
@Slf4j
public class SaslAuthenticationState implements AuthenticationState {
    private final long stateId;
    private static final AtomicLong stateIdGenerator = new AtomicLong(0L);
    private final SaslAuthenticationDataSource authenticationDataSource;
    private PulsarSaslServer pulsarSaslServer;

    public SaslAuthenticationState(PulsarSaslServer server) {
        stateId = stateIdGenerator.incrementAndGet();
        this.authenticationDataSource = new SaslAuthenticationDataSource(server);
        this.pulsarSaslServer = server;
    }

    @Override
    public String getAuthRole() {
        return pulsarSaslServer.getAuthorizationID();
    }

    @Override
    public AuthenticationDataSource getAuthDataSource() {
        return authenticationDataSource;
    }

    @Override
    public boolean isComplete() {
        return pulsarSaslServer.isComplete();
    }

    /**
     * Returns null if authentication has completed, and no auth data is required to send back to client.
     * Do auth and Returns the auth data back to client, if authentication has not completed.
     */
    @Override
    public AuthData authenticate(AuthData authData) throws AuthenticationException {
        return pulsarSaslServer.response(authData);
    }

    @Override
    public long getStateId() {
        return stateId;
    }


}
