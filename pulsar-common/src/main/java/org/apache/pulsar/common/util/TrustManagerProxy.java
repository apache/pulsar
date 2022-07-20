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
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import lombok.extern.slf4j.Slf4j;

/**
 * This class wraps {@link X509ExtendedTrustManager} and gives opportunity to refresh Trust-manager with refreshed certs
 * without changing {@link SslContext}.
 */
@Slf4j
public class TrustManagerProxy extends X509ExtendedTrustManager {

    private volatile X509ExtendedTrustManager trustManager;
    private final FileModifiedTimeUpdater certFile;

    public TrustManagerProxy(String caCertFile, int refreshDurationSec, ScheduledExecutorService executor) {
        this.certFile = new FileModifiedTimeUpdater(caCertFile);
        try {
            updateTrustManager();
        } catch (KeyManagementException | IOException | CertificateException e) {
            log.warn("Failed to load cert {}, {}", certFile, e.getMessage());
            throw new IllegalArgumentException(e);
        } catch (NoSuchAlgorithmException | KeyStoreException e) {
            log.warn("Failed to init trust-store", e);
            throw new IllegalArgumentException(e);
        }
        executor.scheduleWithFixedDelay(() -> updateTrustManagerSafely(), refreshDurationSec, refreshDurationSec,
                TimeUnit.SECONDS);
    }

    private void updateTrustManagerSafely() {
        try {
            updateTrustManager();
        } catch (Exception e) {
            log.warn("Failed to init trust-store {}", certFile.getFileName(), e);
        }
    }

    private void updateTrustManager() throws CertificateException, KeyStoreException, NoSuchAlgorithmException,
            IOException, KeyManagementException {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null);
        X509Certificate[] certificates = SecurityUtility.loadCertificatesFromPemFile(certFile.getFileName());
        for (X509Certificate certificate : certificates) {
            String alias = certificate.getSubjectX500Principal().getName();
            keyStore.setCertificateEntry(alias, certificate);
        }
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);
        trustManager = (X509ExtendedTrustManager) trustManagerFactory.getTrustManagers()[0];
    }

    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        trustManager.checkClientTrusted(x509Certificates, s);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        trustManager.checkServerTrusted(x509Certificates, s);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return trustManager.getAcceptedIssuers();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        trustManager.checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        trustManager.checkClientTrusted(chain, authType, engine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        trustManager.checkServerTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        trustManager.checkServerTrusted(chain, authType, engine);
    }
}
