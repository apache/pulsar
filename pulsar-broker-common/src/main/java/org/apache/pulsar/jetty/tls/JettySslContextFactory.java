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
        return new JettySslContextFactory.Server(sslProviderString, pulsarSslFactory,
                requireTrustedClientCertOnConnect, ciphers, protocols);
    }

    private static class Server extends SslContextFactory.Server {
        private final PulsarSslFactory pulsarSslFactory;

        public Server(String sslProviderString, PulsarSslFactory pulsarSslFactory,
                      boolean requireTrustedClientCertOnConnect, Set<String> ciphers, Set<String> protocols) {
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
            return this.pulsarSslFactory.getInternalSslContext();
        }
    }
}
