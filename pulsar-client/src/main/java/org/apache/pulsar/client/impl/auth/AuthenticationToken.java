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

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.internal.V5AuthenticationProvider;
import org.apache.pulsar.client.impl.auth.v5.TokenAuthenticationV5;

/**
 * Token based authentication provider.
 *
 * <p>The verbatim v4 synchronous surface ({@link #getAuthData()} / {@link AuthenticationDataToken}) is
 * preserved for callers of the v4 API; PIP-478 additionally exposes the v5-native
 * {@link TokenAuthenticationV5} through {@link V5AuthenticationProvider}, which is what the client itself
 * drives — over the non-blocking binary path, without bridging this class.
 */
public class AuthenticationToken
        implements Authentication, EncodedAuthenticationParameterSupport, V5AuthenticationProvider {
    static final String AUTH_METHOD_NAME = "token";

    private static final long serialVersionUID = 1L;
    private Supplier<String> tokenSupplier = null;
    // PIP-478: the client's framework services, late-bound before start(); null until then.

    public AuthenticationToken() {
    }

    public AuthenticationToken(String token) {
        this(new TokenAuthenticationV5.LiteralTokenSupplier(token));
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
        // PIP-478: parse through the v5 body's parser, so `authPluginClassName=...AuthenticationToken`
        // resolving straight to TokenAuthenticationV5 and this shim accept exactly the same forms.
        this.tokenSupplier = TokenAuthenticationV5.tokenSupplier(encodedAuthParamString);
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
    public org.apache.pulsar.client.api.v5.auth.Authentication v5Authentication() {
        // PIP-478: the client drives this v5-native token body; the v4 methods above remain for callers of
        // the v4 API. The supplier is read live so a token rotated via configure(...) is picked up on the
        // next connection attempt.
        return new TokenAuthenticationV5(() -> tokenSupplier.get());
    }

    /**
     * Deserialization shim for an {@code AuthenticationToken} serialized by Pulsar 4.x, whose token supplier
     * was this inner class. The supplier itself now lives on the v5 body
     * ({@link TokenAuthenticationV5.LiteralTokenSupplier}), and Java serialization resolves by class name, so
     * without this shim a 4.x blob fails to deserialize with {@code ClassNotFoundException}. It carries the
     * original {@code serialVersionUID} and resolves to the new supplier on read.
     *
     * @deprecated since 5.0.0; only reachable through deserialization of pre-5.0 data.
     */
    @Deprecated
    private static class SerializableTokenSupplier implements Supplier<String>, Serializable {

        private static final long serialVersionUID = 5095234161799506913L;
        private final String token;

        SerializableTokenSupplier(final String token) {
            this.token = token;
        }

        @Override
        public String get() {
            return token;
        }

        private Object readResolve() {
            return new TokenAuthenticationV5.LiteralTokenSupplier(token);
        }
    }

    /**
     * Deserialization shim for the 4.x file-backed token supplier; see {@link SerializableTokenSupplier}.
     *
     * @deprecated since 5.0.0; only reachable through deserialization of pre-5.0 data.
     */
    @Deprecated
    private static class SerializableURITokenSupplier implements Supplier<String>, Serializable {

        private static final long serialVersionUID = 3160666668166028760L;
        private final URI uri;

        SerializableURITokenSupplier(final URI uri) {
            this.uri = uri;
        }

        @Override
        public String get() {
            return new TokenAuthenticationV5.FileTokenSupplier(uri).get();
        }

        private Object readResolve() {
            return new TokenAuthenticationV5.FileTokenSupplier(uri);
        }
    }
}
