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
package org.apache.pulsar.common.tls.impl;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.netty.handler.ssl.SslContext;
import java.util.List;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;

/**
 * A Netty {@link SslContext} that applies an engine-level {@link SSLParameters} <em>overlay</em> to every
 * {@link SSLEngine} it produces, wrapping a framework-synthesized JDK-backed context (PIP-478).
 *
 * <p>The universal {@code javax.net.ssl.SSLContext} fallback the framework synthesizes a Netty context from is
 * always JDK-backed — a {@link io.netty.handler.ssl.JdkSslContext} wrapping the factory-supplied
 * {@code SSLContext}. A JDK {@code SSLContext} carries no engine-level policy: the endpoint-identification
 * algorithm, the enabled protocols and cipher suites, algorithm constraints, the application protocols, and
 * (server side) the client-auth mode are all {@link SSLParameters} settings, and a bare {@code SSLContext} has
 * no setter for them. So the framework composes the desired baseline — the factory's own {@code SSLParameters}
 * companion (see {@link TlsContexts}), merged with the consumer's hostname-verification / client-auth flags —
 * and applies it here, per engine.
 *
 * <p>This is the faithful equivalent of the native {@code FileBasedTlsFactory} path, which bakes the same
 * settings into the context via {@code SslContextBuilder} at build time — consumers rely on the context to
 * carry the policy and never re-apply it per connection. It is sound because the synthesized context is always
 * the JDK backend, on which a per-{@code SSLEngine} override cleanly takes effect (unlike Netty's OpenSSL
 * backend, which fixes several of these at build time); see PIP-478 Detailed Design,
 * "client-side hostname verification".
 *
 * <p><b>SNI is never overlaid.</b> {@code getSSLParameters()} returns a snapshot of the engine's full
 * configuration — including the SNI {@code serverNames} the delegate already set for this connection from the
 * peer host — and this class overlays only the {@code non-null} baseline members onto that snapshot before
 * writing it back, leaving {@code serverNames} untouched. That realizes merge rule 3: the per-connection SNI
 * always wins over any factory baseline.
 */
final class SynthesizedEngineSslContext extends SslContext {

    private final SslContext delegate;
    private final SSLParameters overlay;
    private final boolean applyClientAuth;

    /**
     * @param delegate        the JDK-backed Netty context whose engines are reconfigured
     * @param overlay         the composed baseline; only its non-null members are applied (and, when
     *                        {@code applyClientAuth} is set, its need/want-client-auth flags)
     * @param applyClientAuth whether to apply the overlay's {@code needClientAuth}/{@code wantClientAuth}
     *                        (server purposes with a factory-supplied baseline — merge rule 4); client
     *                        engines ignore client-auth, so it is left {@code false} for them
     */
    SynthesizedEngineSslContext(SslContext delegate, SSLParameters overlay, boolean applyClientAuth) {
        this.delegate = delegate;
        this.overlay = overlay;
        this.applyClientAuth = applyClientAuth;
    }

    @Override
    public boolean isClient() {
        return delegate.isClient();
    }

    @Override
    public List<String> cipherSuites() {
        return delegate.cipherSuites();
    }

    @Override
    @SuppressWarnings("deprecation") // ApplicationProtocolNegotiator is a deprecated but still-abstract SPI type
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return delegate.applicationProtocolNegotiator();
    }

    @Override
    public SSLSessionContext sessionContext() {
        return delegate.sessionContext();
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator alloc) {
        return applyOverlay(delegate.newEngine(alloc));
    }

    @Override
    public SSLEngine newEngine(ByteBufAllocator alloc, String peerHost, int peerPort) {
        return applyOverlay(delegate.newEngine(alloc, peerHost, peerPort));
    }

    private SSLEngine applyOverlay(SSLEngine engine) {
        SSLParameters engineParams = engine.getSSLParameters();
        if (overlay.getProtocols() != null) {
            engineParams.setProtocols(overlay.getProtocols());
        }
        if (overlay.getCipherSuites() != null) {
            engineParams.setCipherSuites(overlay.getCipherSuites());
        }
        if (overlay.getAlgorithmConstraints() != null) {
            engineParams.setAlgorithmConstraints(overlay.getAlgorithmConstraints());
        }
        if (overlay.getApplicationProtocols() != null) {
            engineParams.setApplicationProtocols(overlay.getApplicationProtocols());
        }
        if (overlay.getEndpointIdentificationAlgorithm() != null) {
            engineParams.setEndpointIdentificationAlgorithm(overlay.getEndpointIdentificationAlgorithm());
        }
        if (applyClientAuth) {
            // Merge rule 4: a factory-supplied SSLParameters is authoritative for the server client-auth mode.
            // needClientAuth wins over wantClientAuth; neither set means "no client certificate requested".
            if (overlay.getNeedClientAuth()) {
                engineParams.setNeedClientAuth(true);
            } else if (overlay.getWantClientAuth()) {
                engineParams.setWantClientAuth(true);
            } else {
                engineParams.setWantClientAuth(false);
            }
        }
        // serverNames deliberately left as the delegate set them (per-connection SNI wins — merge rule 3).
        engine.setSSLParameters(engineParams);
        return engine;
    }
}
