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

/**
 * The per-consumer settings the framework bakes into a Netty {@code io.netty.handler.ssl.SslContext} when it
 * synthesizes one from a factory's {@code javax.net.ssl.SSLContext} fallback (PIP-478).
 *
 * <p>Synthesis happens only when a <em>custom</em> factory returns {@code Optional.empty()} for the Netty
 * class but supplies the JDK {@code SSLContext}. In that case the framework does not know the factory's
 * internal {@code TlsPolicy}, so the consumer must supply the two role-specific settings its configuration
 * dictates:
 * <ul>
 *   <li><b>Client purposes</b> — {@code enableHostnameVerification}: whether the produced engines verify the
 *       peer hostname ({@code endpointIdentificationAlgorithm = "HTTPS"}). Threaded from the consumer's own
 *       hostname-verification flag ({@code tlsHostnameVerificationEnable} on the client;
 *       {@code tlsHostnameVerificationEnabled} on the proxy&rarr;broker path).</li>
 *   <li><b>Server purposes</b> — {@code requireTrustedClientCert}: whether client auth is required
 *       ({@code ClientAuth.REQUIRE}) or merely requested ({@code ClientAuth.OPTIONAL}). Threaded from
 *       {@code tlsRequireTrustedClientCertOnConnect}.</li>
 * </ul>
 * The acquisition helper selects which field applies from the purpose's {@link
 * org.apache.pulsar.tls.TlsPurpose#role() role}, so a caller passes only the setting relevant to the
 * purpose it is acquiring via {@link #client(boolean)} or {@link #server(boolean)}.
 *
 * @param enableHostnameVerification client-purpose hostname verification (ignored for server purposes)
 * @param requireTrustedClientCert   server-purpose client-auth requirement (ignored for client purposes)
 */
public record TlsSynthesisSpec(boolean enableHostnameVerification, boolean requireTrustedClientCert) {

    /**
     * A client-purpose synthesis spec.
     *
     * @param enableHostnameVerification whether the synthesized client engines verify the peer hostname
     * @return the spec
     */
    public static TlsSynthesisSpec client(boolean enableHostnameVerification) {
        return new TlsSynthesisSpec(enableHostnameVerification, false);
    }

    /**
     * A server-purpose synthesis spec.
     *
     * @param requireTrustedClientCert whether the synthesized server context requires a trusted client cert
     * @return the spec
     */
    public static TlsSynthesisSpec server(boolean requireTrustedClientCert) {
        return new TlsSynthesisSpec(false, requireTrustedClientCert);
    }
}
