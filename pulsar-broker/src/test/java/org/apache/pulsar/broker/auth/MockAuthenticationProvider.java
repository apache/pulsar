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
package org.apache.pulsar.broker.auth;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.io.IOException;
import java.net.SocketAddress;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockAuthenticationProvider implements AuthenticationProvider {
    private static final Logger log = LoggerFactory.getLogger(MockAuthenticationProvider.class);

    @Override
    public void close() throws IOException {}

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {}

    @Override
    public String getAuthMethodName() {
        // method name
        return "mock";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        String principal = "unknown";
        if (authData.hasDataFromHttp()) {
            principal = authData.getHttpHeader("mockuser");
        } else if (authData.hasDataFromCommand()) {
            principal = authData.getCommandData();
        }

        String[] parts = principal.split("\\.");
        if (parts.length == 2) {
            switch (parts[0]) {
                case "pass":
                    return principal;
                case "fail":
                    throw new AuthenticationException("Do not pass");
                case "error":
                    throw new RuntimeException("Error in authn");
            }
        }
        throw new IllegalArgumentException(
                "Not a valid principle. Should be [pass|fail|error].[pass|fail|error], found " + principal);
    }

    @Override
    public AuthenticationState newAuthState(AuthData authData, SocketAddress remoteAddress, SSLSession sslSession)
            throws AuthenticationException {
        return new MockAuthState(this);
    }

    private static class MockAuthState implements AuthenticationState {
        private String authRole = null;
        private AuthenticationDataSource dataSource = null;
        private final MockAuthenticationProvider provider;

        public MockAuthState(MockAuthenticationProvider provider) {
            this.provider = provider;
        }

        @Override
        public String getAuthRole() {
            return authRole;
        }

        @Override
        public AuthData authenticate(AuthData authData) throws AuthenticationException {
            this.dataSource = new AuthenticationDataCommand(new String(authData.getBytes(), UTF_8));
            this.authRole = provider.authenticate(this.dataSource);
            return null;
        }

        @Override
        public AuthenticationDataSource getAuthDataSource() {
            return dataSource;
        }

        @Override
        public boolean isComplete() {
            return true;
        }
    }
}
