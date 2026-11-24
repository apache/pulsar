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
package org.apache.pulsar.client.impl.auth;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.Map;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.internal.V5AuthenticationProvider;
import org.apache.pulsar.client.impl.auth.v5.BasicAuthenticationV5;

/**
 * HTTP-basic authentication provider.
 *
 * <p>The verbatim v4 synchronous surface ({@link #getAuthData()} / {@link AuthenticationDataBasic}) is
 * preserved for callers of the v4 API; PIP-478 additionally exposes the v5-native
 * {@link BasicAuthenticationV5} through {@link V5AuthenticationProvider}, which is what the client itself
 * drives — over the non-blocking binary path, without bridging this class.
 */
public class AuthenticationBasic
        implements Authentication, EncodedAuthenticationParameterSupport, V5AuthenticationProvider {
    static final String AUTH_METHOD_NAME = "basic";
    private String userId;
    private String password;
    // PIP-478: the client's framework services, late-bound before start(); null until then.

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public String getAuthMethodName() {
        return AUTH_METHOD_NAME;
    }

    @SuppressWarnings("deprecation")
    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        try {
            return new AuthenticationDataBasic(userId, password);
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void configure(Map<String, String> authParams) {
        configure(new Gson().toJson(authParams));
    }

    @Override
    public void configure(String encodedAuthParamString) {
        JsonObject params = new Gson().fromJson(encodedAuthParamString, JsonObject.class);
        userId = params.get("userId").getAsString();
        password = params.get("password").getAsString();
    }

    @Override
    public void start() throws PulsarClientException {
        // noop
    }

    @Override
    public org.apache.pulsar.client.api.v5.auth.Authentication v5Authentication() {
        // PIP-478: the client drives this v5-native basic body; the v4 methods above remain for callers of
        // the v4 API. The fields are read live, so a credential changed by a later configure(...) on this
        // same instance is picked up on the next connection attempt.
        return new BasicAuthenticationV5(() -> userId, () -> password);
    }

}
