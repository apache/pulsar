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

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function;

import javax.naming.AuthenticationException;

import static org.apache.pulsar.broker.authentication.AuthenticationProviderToken.HTTP_HEADER_VALUE_PREFIX;
import static org.apache.pulsar.client.impl.auth.AuthenticationDataToken.HTTP_HEADER_NAME;

public class ClearTextFunctionTokenAuthProvider implements FunctionAuthProvider {
    @Override
    public void configureAuthenticationConfig(AuthenticationConfig authConfig, Function.FunctionAuthenticationSpec functionAuthenticationSpec) {
        authConfig.setClientAuthenticationPlugin(AuthenticationToken.class.getName());
        authConfig.setClientAuthenticationParameters("token://" + functionAuthenticationSpec.getData());
    }

    @Override
    public Function.FunctionAuthenticationSpec cacheAuthData(String tenant, String namespace, String name, AuthenticationDataSource authenticationDataSource) throws Exception {
        String token = null;
        try {
            token = getToken(authenticationDataSource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (token != null) {
            return Function.FunctionAuthenticationSpec.newBuilder().setData(token).build();
        }
        return null;
    }

    @Override
    public void cleanUpAuthData(String tenant, String namespace, String name, Function.FunctionAuthenticationSpec
            functionAuthenticationSpec) throws Exception {
        //no-op
    }

    private String getToken(AuthenticationDataSource authData) throws AuthenticationException {
        if (authData.hasDataFromCommand()) {
            // Authenticate Pulsar binary connection
            return authData.getCommandData();
        } else if (authData.hasDataFromHttp()) {
            // Authentication HTTP request. The format here should be compliant to RFC-6750
            // (https://tools.ietf.org/html/rfc6750#section-2.1). Eg: Authorization: Bearer xxxxxxxxxxxxx
            String httpHeaderValue = authData.getHttpHeader(HTTP_HEADER_NAME);
            if (httpHeaderValue == null || !httpHeaderValue.startsWith(HTTP_HEADER_VALUE_PREFIX)) {
                throw new AuthenticationException("Invalid HTTP Authorization header");
            }

            // Remove prefix
            String token = httpHeaderValue.substring(HTTP_HEADER_VALUE_PREFIX.length());
            return validateToken(token);
        } else {
            return null;
        }
    }

    private String validateToken(final String token) throws AuthenticationException {
        if (StringUtils.isNotBlank(token)) {
            return token;
        } else {
            throw new AuthenticationException("Blank token found");
        }
    }
}
