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
package org.apache.pulsar.functions.auth;

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.functions.instance.AuthenticationConfig;

import java.util.Optional;

import static org.apache.pulsar.broker.authentication.AuthenticationProviderToken.getToken;

public class ClearTextFunctionTokenAuthProvider implements FunctionAuthProvider {
    @Override
    public void configureAuthenticationConfig(AuthenticationConfig authConfig, FunctionAuthData functionAuthData) {
        authConfig.setClientAuthenticationPlugin(AuthenticationToken.class.getName());
        authConfig.setClientAuthenticationParameters("token:" + new String(functionAuthData.getData()));
    }

    @Override
    public Optional<FunctionAuthData> cacheAuthData(String tenant, String namespace, String name, AuthenticationDataSource authenticationDataSource) throws Exception {
        String token = null;
        try {
            token = getToken(authenticationDataSource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (token != null) {
            return Optional.of(FunctionAuthData.builder().data(token.getBytes()).build());
        }
        return null;
    }

    @Override
    public void cleanUpAuthData(String tenant, String namespace, String name, FunctionAuthData functionAuthData) throws Exception {
        //no-op
    }
}
