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
package org.apache.pulsar.common.util;

import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:JavadocType")
public class ClientSslContextRefresher {
    private volatile SslContext sslContext;
    private boolean tlsAllowInsecureConnection;
    private String tlsTrustCertsFilePath;
    private AuthenticationDataProvider authData;

    public ClientSslContextRefresher(boolean allowInsecure, String trustCertsFilePath,
            AuthenticationDataProvider authData) throws IOException, GeneralSecurityException {
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = trustCertsFilePath;
        this.authData = authData;

        if (authData != null && authData.hasDataForTls()) {
            this.sslContext = SecurityUtility.createNettySslContextForClient(this.tlsAllowInsecureConnection,
                    this.tlsTrustCertsFilePath, (X509Certificate[]) authData.getTlsCertificates(),
                    authData.getTlsPrivateKey());
        } else {
            this.sslContext = SecurityUtility.createNettySslContextForClient(this.tlsAllowInsecureConnection,
                    this.tlsTrustCertsFilePath);
        }
    }

    public SslContext get() {
        if (authData != null && authData.hasDataForTls()) {
            try {
                this.sslContext = SecurityUtility.createNettySslContextForClient(this.tlsAllowInsecureConnection,
                        this.tlsTrustCertsFilePath, (X509Certificate[]) authData.getTlsCertificates(),
                        authData.getTlsPrivateKey());
            } catch (GeneralSecurityException | IOException e) {
                LOG.error("Exception occured while trying to refresh sslContext: ", e);
            }

        }
        return sslContext;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ClientSslContextRefresher.class);
}
