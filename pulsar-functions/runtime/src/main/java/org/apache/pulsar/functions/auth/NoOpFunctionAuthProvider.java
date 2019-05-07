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
import org.apache.pulsar.functions.instance.AuthenticationConfig;

import java.util.Optional;

public class NoOpFunctionAuthProvider implements FunctionAuthProvider{
    @Override
    public void configureAuthenticationConfig(AuthenticationConfig authConfig, Optional<FunctionAuthData> functionAuthData) {

    }

    @Override
    public Optional<FunctionAuthData> cacheAuthData(String tenant, String namespace, String name,
                                                    AuthenticationDataSource authenticationDataSource)
            throws Exception {
        return Optional.empty();
    }

    @Override
    public Optional<FunctionAuthData> updateAuthData(String tenant, String namespace, String name,
                                                     Optional<FunctionAuthData> existingFunctionAuthData, AuthenticationDataSource authenticationDataSource) throws Exception {
        return Optional.empty();
    }

    @Override
    public void cleanUpAuthData(String tenant, String namespace, String name, Optional<FunctionAuthData> functionAuthData) throws Exception {

    }
}
