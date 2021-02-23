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

import java.io.IOException;

import javax.naming.AuthenticationException;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;

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

}
