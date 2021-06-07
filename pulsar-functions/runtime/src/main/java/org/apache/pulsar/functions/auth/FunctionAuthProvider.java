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
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.common.util.Reflections;

import java.util.Optional;

/**
 * This is a generic interface that functions can use to cache and distribute appropriate authentication
 * data that is needed to configure the runtime of functions to support appropriate authentication of function instances
 */
public interface FunctionAuthProvider {

    /**
     * Set authentication configs for function instance based on the data in FunctionAuthenticationSpec
     * @param authConfig authentication configs passed to the function instance
     * @param functionAuthData function authentication data that is provider specific
     */
    void configureAuthenticationConfig(AuthenticationConfig authConfig, Optional<FunctionAuthData> functionAuthData);

    /**
     * Cache auth data in as part of function metadata for function that runtime may need to configure authentication
     * @param funcDetails the function details
     * @param authenticationDataSource auth data
     * @return
     * @throws Exception
     */
    Optional<FunctionAuthData> cacheAuthData(Function.FunctionDetails funcDetails, AuthenticationDataSource authenticationDataSource) throws Exception;

    Optional<FunctionAuthData> updateAuthData(Function.FunctionDetails funcDetails, Optional<FunctionAuthData> existingFunctionAuthData, AuthenticationDataSource authenticationDataSource) throws Exception;

    /**
     * Clean up operation for auth when function is terminated
     * @param funcDetails the function details
     * @param functionAuthData function auth data
     * @throws Exception
     */
    void cleanUpAuthData(Function.FunctionDetails funcDetails, Optional<FunctionAuthData> functionAuthData) throws Exception;

    static FunctionAuthProvider getAuthProvider(String className) {
        return Reflections.createInstance(className, FunctionAuthProvider.class, Thread.currentThread().getContextClassLoader());
    }
}
