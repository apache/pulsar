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
package org.apache.pulsar.sql.presto;

import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.PasswordAuthenticator;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProviderBasic;

import javax.naming.AuthenticationException;
import java.io.IOException;
import java.security.Principal;

public class PulsarPasswordAuthenticator implements PasswordAuthenticator {

    private final AuthenticationProviderBasic authProvider = new AuthenticationProviderBasic();

    public void initialize() throws IOException {
        authProvider.initialize(null);
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password) {
        try {
            String userId = authProvider.authenticate(new AuthenticationDataSource() {
                @Override
                public boolean hasDataFromCommand() {
                    return true;
                }

                @Override
                public String getCommandData() {
                    return String.format("%s:%s", user, password);
                }
            });
            return () -> userId;
        } catch (AuthenticationException e) {
            throw new AccessDeniedException(e.getMessage());
        }
    }
}
