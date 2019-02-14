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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.SslContext;

public class ServerSslContextRefresher {
    private final boolean tlsAllowInsecureConnection;
    private final FileModifiedTimeUpdater tlsTrustCertsFilePath, tlsCertificateFilePath, tlsKeyFilePath;
    private final Set<String> tlsCiphers;
    private final Set<String> tlsProtocols;
    private final boolean tlsRequireTrustedClientCertOnConnect;
    private final long delayInMins;
    private long nextRefreshTimeInMins;
    private volatile SslContext sslContext;

    public ServerSslContextRefresher(boolean allowInsecure, String trustCertsFilePath, String certificateFilePath,
            String keyFilePath, Set<String> ciphers, Set<String> protocols, boolean requireTrustedClientCertOnConnect,
            long delayInMins) throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = new FileModifiedTimeUpdater(trustCertsFilePath);
        this.tlsCertificateFilePath = new FileModifiedTimeUpdater(certificateFilePath);
        this.tlsKeyFilePath = new FileModifiedTimeUpdater(keyFilePath);
        this.tlsCiphers = ciphers;
        this.tlsProtocols = protocols;
        this.tlsRequireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;
        this.delayInMins = delayInMins;
        this.nextRefreshTimeInMins = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + delayInMins;

        buildSSLContext();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Certs will be refreshed every {} minutes", delayInMins);
        }
    }

    public void buildSSLContext() throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        this.sslContext = SecurityUtility.createNettySslContextForServer(tlsAllowInsecureConnection,
                tlsTrustCertsFilePath.getFileName(), tlsCertificateFilePath.getFileName(), tlsKeyFilePath.getFileName(),
                tlsCiphers, tlsProtocols, tlsRequireTrustedClientCertOnConnect);
    }

    public SslContext get() {
        long nowInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        if (nextRefreshTimeInMins > nowInSeconds) {
            nextRefreshTimeInMins = nowInSeconds + delayInMins;
            if (tlsTrustCertsFilePath.checkAndRefresh() || tlsCertificateFilePath.checkAndRefresh()
                    || tlsKeyFilePath.checkAndRefresh()) {
                try {
                    buildSSLContext();
                } catch (GeneralSecurityException | IOException e) {
                    LOG.error("Execption while trying to refresh ssl Context: ", e);
                }
            }
        }
        return this.sslContext;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ServerSslContextRefresher.class);
}
