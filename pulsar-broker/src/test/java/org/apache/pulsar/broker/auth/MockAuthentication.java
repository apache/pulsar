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

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockAuthentication implements Authentication {
    private static final Logger log = LoggerFactory.getLogger(MockAuthentication.class);
    private final String user;

    public MockAuthentication(String user) {
        this.user = user;
    }

    @Override
    public void close() {}

    @Override
    public String getAuthMethodName() {
        return "mock";
    }

    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        return new AuthenticationDataProvider() {
            @Override
            public boolean hasDataForHttp() { return true; }
            @Override
            public String getHttpAuthType() { return "mock"; }
            @Override
            public Set<Map.Entry<String, String>> getHttpHeaders() {
                return ImmutableMap.of("mockuser", user).entrySet();
            }
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }
            @Override
            public String getCommandData() {
                return user;
            }
        };
    }

    @Override
    public void configure(Map<String, String> authParams) {
    }

    @Override
    public void start() throws PulsarClientException {}


    @Override
    public void authenticationStage(String requestUrl,
                                     AuthenticationDataProvider authData,
                                     Map<String, String> previousResHeaders,
                                     CompletableFuture<Map<String, String>> authFuture) {
        authFuture.complete(null);
    }
}
