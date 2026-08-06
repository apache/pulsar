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
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver.AuthenticationExchange;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServicesAware;
import org.apache.pulsar.client.impl.auth.v5.TokenAuthenticationV5;
import org.apache.pulsar.client.impl.auth.v5.V5BinaryAuthenticationDriver;

/**
 * Token based authentication provider.
 *
 * <p>The verbatim v4 synchronous surface ({@link #getAuthData()} / {@link AuthenticationDataToken}) is
 * preserved; PIP-478 additionally exposes {@link AsyncAuthenticationDriver} so {@code ClientCnx} can drive
 * the token credential over the non-blocking binary path via the v5-native {@link TokenAuthenticationV5}.
 */
public class AuthenticationToken
        implements Authentication, EncodedAuthenticationParameterSupport, AsyncAuthenticationDriver,
        ClientAuthenticationServicesAware {
    static final String AUTH_METHOD_NAME = "token";

    private static final long serialVersionUID = 1L;
    private Supplier<String> tokenSupplier = null;
    // PIP-478: the client's framework services, late-bound before start(); null until then.
    private transient volatile ClientAuthenticationServices authServices;

    public AuthenticationToken() {
    }

    public AuthenticationToken(String token) {
        this(new SerializableTokenSupplier(token));
    }

    public AuthenticationToken(Supplier<String> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
    }

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
        return new AuthenticationDataToken(tokenSupplier);
    }

    @Override
    public void configure(String encodedAuthParamString) {
        // Interpret the whole param string as the token. If the string contains the notation `token:xxxxx` then strip
        // the prefix
        if (encodedAuthParamString.startsWith("token:")) {
            this.tokenSupplier = new SerializableTokenSupplier(encodedAuthParamString.substring("token:".length()));
        } else if (encodedAuthParamString.startsWith("file:")) {
            // Read token from a file
            URI filePath = URI.create(encodedAuthParamString);
            this.tokenSupplier = new SerializableURITokenSupplier(filePath);
        } else {
            try {
                // Read token from json string
                JsonObject authParams = new Gson().fromJson(encodedAuthParamString, JsonObject.class);
                this.tokenSupplier = new SerializableTokenSupplier(authParams.get("token").getAsString());
            } catch (JsonSyntaxException e) {
                this.tokenSupplier = new SerializableTokenSupplier(encodedAuthParamString);
            }
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void configure(Map<String, String> authParams) {
        // noop
    }

    @Override
    public void start() throws PulsarClientException {
        // noop
    }

    @Override
    public void bindClientAuthenticationServices(ClientAuthenticationServices services) {
        this.authServices = services;
    }

    @Override
    public AuthenticationExchange newAuthenticationExchange(String brokerHostName) {
        // PIP-478: drive the v5-native token body on the async binary path. The supplier is read live so a
        // token rotated via configure(...) is picked up on the next connection attempt.
        return new V5BinaryAuthenticationDriver(new TokenAuthenticationV5(() -> tokenSupplier.get()), authServices)
                .newAuthenticationExchange(brokerHostName);
    }

    private static class SerializableURITokenSupplier implements Supplier<String>, Serializable {

        private static final long serialVersionUID = 3160666668166028760L;
        private final URI uri;

        public SerializableURITokenSupplier(final URI uri) {
            super();
            this.uri = uri;
        }

        @Override
        public String get() {
            try {
                return new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8).trim();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read token from file", e);
            }
        }
    }

    private static class SerializableTokenSupplier implements Supplier<String>, Serializable {

        private static final long serialVersionUID = 5095234161799506913L;
        private final String token;

        public SerializableTokenSupplier(final String token) {
            super();
            this.token = token;
        }

        @Override
        public String get() {
            return token;
        }

    }
}
