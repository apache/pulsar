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
package org.apache.pulsar;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;

public class MockTokenAuthenticationProvider implements AuthenticationProvider {

    public static final String KEY = "role";

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        // No ops
    }

    @Override
    public String getAuthMethodName() {
        return "mock";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        if (!authData.hasDataFromHttp()) {
            throw new AuthenticationException("No HTTP data in " + authData);
        }
        String value = authData.getHttpHeader(KEY);
        if (value == null) {
            throw new AuthenticationException("No HTTP header for " + KEY);
        }
        return value;
    }

    @Override
    public void close() throws IOException {
        // No ops
    }

    public static class MockAuthentication implements Authentication {

        @Override
        public AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
            return new MockAuthenticationData();
        }

        @Override
        public String getAuthMethodName() {
            return "mock";
        }

        @Override
        public void configure(Map<String, String> authParams) {
            // No ops
        }

        @Override
        public void start() throws PulsarClientException {
            // No ops
        }

        @Override
        public void close() throws IOException {
            // No ops
        }
    }

    private static class MockAuthenticationData implements AuthenticationDataProvider {

        @Override
        public boolean hasDataForHttp() {
            return true;
        }

        @Override
        public Set<Map.Entry<String, String>> getHttpHeaders() throws Exception {
            return Collections.singletonMap(KEY, "admin").entrySet();
        }
    }
}
