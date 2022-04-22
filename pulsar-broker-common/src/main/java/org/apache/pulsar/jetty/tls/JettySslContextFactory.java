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
package org.apache.pulsar.jetty.tls;

import java.util.Set;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.DefaultSslContextBuilder;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NetSslContextBuilder;
import org.eclipse.jetty.util.ssl.SslContextFactory;

@Slf4j
public class JettySslContextFactory {
    static {
        // DO NOT EDIT - Load Conscrypt provider
        if (SecurityUtility.CONSCRYPT_PROVIDER != null) {
        }
    }

    public static SslContextFactory.Server createServerSslContextWithKeystore(String sslProviderString,
                                                                              String keyStoreTypeString,
                                                                              String keyStore,
                                                                              String keyStorePassword,
                                                                              boolean allowInsecureConnection,
                                                                              String trustStoreTypeString,
                                                                              String trustStore,
                                                                              String trustStorePassword,
                                                                              boolean requireTrustedClientCertOnConnect,
                                                                              Set<String> ciphers,
                                                                              Set<String> protocols,
                                                                              long certRefreshInSec) {
        NetSslContextBuilder sslCtxRefresher = new NetSslContextBuilder(
                sslProviderString,
                keyStoreTypeString,
                keyStore,
                keyStorePassword,
                allowInsecureConnection,
                trustStoreTypeString,
                trustStore,
                trustStorePassword,
                requireTrustedClientCertOnConnect,
                certRefreshInSec);

        return new JettySslContextFactory.Server(sslProviderString, sslCtxRefresher,
                requireTrustedClientCertOnConnect, ciphers, protocols);
    }

    public static SslContextFactory createServerSslContext(String sslProviderString, boolean tlsAllowInsecureConnection,
                                                           String tlsTrustCertsFilePath,
                                                           String tlsCertificateFilePath,
                                                           String tlsKeyFilePath,
                                                           boolean tlsRequireTrustedClientCertOnConnect,
                                                           Set<String> ciphers,
                                                           Set<String> protocols,
                                                           long certRefreshInSec) {
        DefaultSslContextBuilder sslCtxRefresher =
                new DefaultSslContextBuilder(tlsAllowInsecureConnection, tlsTrustCertsFilePath, tlsCertificateFilePath,
                        tlsKeyFilePath, tlsRequireTrustedClientCertOnConnect, certRefreshInSec, sslProviderString);

        return new JettySslContextFactory.Server(sslProviderString, sslCtxRefresher,
                tlsRequireTrustedClientCertOnConnect, ciphers, protocols);
    }

    private static class Server extends SslContextFactory.Server {
        private final SslContextAutoRefreshBuilder<SSLContext> sslCtxRefresher;

        public Server(String sslProviderString, SslContextAutoRefreshBuilder<SSLContext> sslCtxRefresher,
                      boolean requireTrustedClientCertOnConnect, Set<String> ciphers, Set<String> protocols) {
            super();
            this.sslCtxRefresher = sslCtxRefresher;

            if (ciphers != null && ciphers.size() > 0) {
                this.setIncludeCipherSuites(ciphers.toArray(new String[0]));
            }

            if (protocols != null && protocols.size() > 0) {
                this.setIncludeProtocols(protocols.toArray(new String[0]));
            }

            if (sslProviderString != null && !sslProviderString.equals("")) {
                setProvider(sslProviderString);
            }

            if (requireTrustedClientCertOnConnect) {
                this.setNeedClientAuth(true);
                this.setTrustAll(false);
            } else {
                this.setWantClientAuth(true);
                this.setTrustAll(true);
            }
        }

        @Override
        public SSLContext getSslContext() {
            return sslCtxRefresher.get();
        }
    }
}
