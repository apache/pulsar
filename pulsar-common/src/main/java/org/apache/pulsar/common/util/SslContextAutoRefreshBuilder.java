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

/**
 * Auto refresher and builder of SSLContext.
 *
 * @param <T>
 *            type of SSLContext
 */
public abstract class SslContextAutoRefreshBuilder<T> {
    protected final boolean tlsAllowInsecureConnection;
    protected final FileModifiedTimeUpdater tlsTrustCertsFilePath, tlsCertificateFilePath, tlsKeyFilePath;
    protected final Set<String> tlsCiphers;
    protected final Set<String> tlsProtocols;
    protected final boolean tlsRequireTrustedClientCertOnConnect;
    protected final long refreshTime;
    protected long lastRefreshTime;

    public SslContextAutoRefreshBuilder(boolean allowInsecure, String trustCertsFilePath, String certificateFilePath,
            String keyFilePath, Set<String> ciphers, Set<String> protocols, boolean requireTrustedClientCertOnConnect,
            long certRefreshInSec) throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = new FileModifiedTimeUpdater(trustCertsFilePath);
        this.tlsCertificateFilePath = new FileModifiedTimeUpdater(certificateFilePath);
        this.tlsKeyFilePath = new FileModifiedTimeUpdater(keyFilePath);
        this.tlsCiphers = ciphers;
        this.tlsProtocols = protocols;
        this.tlsRequireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;
        this.refreshTime = TimeUnit.SECONDS.toMillis(certRefreshInSec);
        this.lastRefreshTime = -1;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Certs will be refreshed every {} seconds", certRefreshInSec);
        }
    }

    /**
     * updates and returns cached SSLContext.
     *
     * @return
     * @throws GeneralSecurityException
     * @throws IOException
     */
    protected abstract T update() throws GeneralSecurityException, IOException;

    /**
     * Returns cached SSLContext.
     *
     * @return
     */
    protected abstract T getSslContext();

    /**
     * It updates SSLContext at every configured refresh time and returns updated SSLContext.
     *
     * @return
     */
    public T get() {
        T ctx = getSslContext();
        if (ctx == null) {
            try {
                update();
                lastRefreshTime = System.currentTimeMillis();
                return getSslContext();
            } catch (GeneralSecurityException | IOException e) {
                LOG.error("Execption while trying to refresh ssl Context {}", e.getMessage(), e);
            }
        } else {
            long now = System.currentTimeMillis();
            if (refreshTime <= 0 || now > (lastRefreshTime + refreshTime)) {
                if (tlsTrustCertsFilePath.checkAndRefresh() || tlsCertificateFilePath.checkAndRefresh()
                        || tlsKeyFilePath.checkAndRefresh()) {
                    try {
                        ctx = update();
                        lastRefreshTime = now;
                    } catch (GeneralSecurityException | IOException e) {
                        LOG.error("Execption while trying to refresh ssl Context {} ", e.getMessage(), e);
                    }
                }
            }
        }
        return ctx;
    }

    private static final Logger LOG = LoggerFactory.getLogger(SslContextAutoRefreshBuilder.class);
}
