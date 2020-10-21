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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Token based authentication provider.
 */
public class AuthenticationToken implements Authentication, EncodedAuthenticationParameterSupport {

    private static final long serialVersionUID = 1L;
    private transient Supplier<String> tokenSupplier;

    public AuthenticationToken() {
    }

    public AuthenticationToken(String token) {
        this(() -> token);
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
            this.tokenSupplier = () -> encodedAuthParamString.substring("token:".length());
        } else if (encodedAuthParamString.startsWith("file:")) {
            // Read token from a file
            URI filePath = URI.create(encodedAuthParamString);
            this.tokenSupplier = () -> {
                try {
                    return new String(Files.readAllBytes(Paths.get(filePath)), Charsets.UTF_8).trim();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read token from file", e);
                }
            };
        } else {
            this.tokenSupplier = () -> encodedAuthParamString;
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

}
