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

package org.apache.pulsar.client.impl.auth;

import com.google.common.base.Charsets;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Supplier;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Token based authentication provider.
 */
public class AuthenticationToken implements Authentication, EncodedAuthenticationParameterSupport {

    private static final long serialVersionUID = 1L;
    private Supplier<String> tokenSupplier = null;

    public AuthenticationToken() {
    }

    public AuthenticationToken(String token) {
        this(new SerializableTokenSupplier(token));
    }

    public AuthenticationToken(Supplier<String> tokenSupplier) { this.tokenSupplier = tokenSupplier; }

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public String getAuthMethodName() {
        return "token";
    }

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

    @Override
    public void configure(Map<String, String> authParams) {
        // noop
    }

    @Override
    public void start() throws PulsarClientException {
        // noop
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
                return new String(Files.readAllBytes(Paths.get(uri)), Charsets.UTF_8).trim();
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
        public String get() { return token; }

    }
}
