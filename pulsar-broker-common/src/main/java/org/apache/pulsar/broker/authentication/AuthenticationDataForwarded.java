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
package org.apache.pulsar.broker.authentication;

/**
 * Authentication data for a forwarded principal without authenticated original credentials.
 */
public final class AuthenticationDataForwarded implements AuthenticationDataSource {
    public static final AuthenticationDataForwarded INSTANCE = new AuthenticationDataForwarded(null);

    private final AuthenticationDataSource proxiedRequestData;

    private AuthenticationDataForwarded(AuthenticationDataSource proxiedRequestData) {
        this.proxiedRequestData = proxiedRequestData;
    }

    /**
     * Returns forwarded data for the original principal of a proxied HTTP request, keeping the request data.
     */
    public static AuthenticationDataForwarded ofProxiedRequest(AuthenticationDataSource requestData) {
        return requestData == null ? INSTANCE : new AuthenticationDataForwarded(requestData);
    }

    /**
     * The authentication data of the proxied request, which may carry the original principal's own token, or null.
     * It is not authenticated as the original principal.
     */
    public AuthenticationDataSource getProxiedRequestData() {
        return proxiedRequestData;
    }
}
