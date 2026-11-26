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
package org.apache.pulsar.jetty.tls;

import java.util.Set;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.tls.TlsClientAuthMode;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.eclipse.jetty.util.ssl.SslContextFactory;

@Slf4j
public class JettySslContextFactory {
    static {
        // DO NOT EDIT - Load Conscrypt provider
        if (SecurityUtility.CONSCRYPT_PROVIDER != null) {
        }
    }

    public static SslContextFactory.Server createSslContextFactory(String sslProviderString,
                                                                   PulsarSslFactory pulsarSslFactory,
                                                                   boolean requireTrustedClientCertOnConnect,
                                                                   Set<String> ciphers, Set<String> protocols) {
        return createSslContextFactory(sslProviderString, pulsarSslFactory,
                requireTrustedClientCertOnConnect ? TlsClientAuthMode.REQUIRE : TlsClientAuthMode.OPTIONAL,
                ciphers, protocols);
    }

    /**
     * Create a Jetty server {@link SslContextFactory} backed by the given {@link PulsarSslFactory}.
     *
     * @param sslProviderString the JSSE provider name, or {@code null}/empty for the default
     * @param pulsarSslFactory  the factory that supplies the {@code SSLContext}
     * @param clientAuthMode    whether to require, request or not request a client certificate
     * @param ciphers           enabled cipher suites, or {@code null} for defaults
     * @param protocols         enabled protocols, or {@code null} for defaults
     * @return the Jetty server SSL context factory
     */
    public static SslContextFactory.Server createSslContextFactory(String sslProviderString,
                                                                   PulsarSslFactory pulsarSslFactory,
                                                                   TlsClientAuthMode clientAuthMode,
                                                                   Set<String> ciphers, Set<String> protocols) {
        return new JettySslContextFactory.Server(sslProviderString, pulsarSslFactory,
                clientAuthMode, ciphers, protocols);
    }

    private static class Server extends SslContextFactory.Server {
        private final PulsarSslFactory pulsarSslFactory;

        public Server(String sslProviderString, PulsarSslFactory pulsarSslFactory,
                      TlsClientAuthMode clientAuthMode, Set<String> ciphers, Set<String> protocols) {
            super();
            this.pulsarSslFactory = pulsarSslFactory;

            if (ciphers != null && ciphers.size() > 0) {
                this.setIncludeCipherSuites(ciphers.toArray(new String[0]));
            }

            if (protocols != null && protocols.size() > 0) {
                this.setIncludeProtocols(protocols.toArray(new String[0]));
            }

            if (sslProviderString != null && !sslProviderString.equals("")) {
                setProvider(sslProviderString);
            }

            if (clientAuthMode == TlsClientAuthMode.REQUIRE) {
                this.setNeedClientAuth(true);
                this.setTrustAll(false);
            } else if (clientAuthMode == TlsClientAuthMode.NONE) {
                // No client certificate is requested, so there is none to trust.
                this.setNeedClientAuth(false);
                this.setWantClientAuth(false);
                this.setTrustAll(false);
            } else {
                this.setWantClientAuth(true);
                this.setTrustAll(true);
            }

            // https://jetty.org/docs/jetty/12.1/operations-guide/protocols/index.html#ssl-sni
            // Set to false for backwards compatibility with Jetty 9.x
            setSniRequired(false);
        }

        @Override
        public SSLContext getSslContext() {
            return this.pulsarSslFactory.getInternalSslContext();
        }
    }
}
