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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import io.netty.handler.ssl.SslContext;
import lombok.extern.slf4j.Slf4j;

/**
 * This class wraps {@link X509ExtendedKeyManager} and gives opportunity to refresh key-manager with refreshed certs
 * without changing {@link SslContext}.
 */
@Slf4j
public class KeyManagerProxy extends X509ExtendedKeyManager {

    private static final char[] KEYSTORE_PASSWORD = "secret".toCharArray();
    private volatile X509ExtendedKeyManager keyManager;
    private FileModifiedTimeUpdater certFile, keyFile;

    public KeyManagerProxy(String certFilePath, String keyFilePath, int refreshDurationSec,
            ScheduledExecutorService executor) {
        this.certFile = new FileModifiedTimeUpdater(certFilePath);
        this.keyFile = new FileModifiedTimeUpdater(keyFilePath);
        try {
            updateKeyManager();
        } catch (CertificateException e) {
            log.warn("Failed to load cert {}", certFile, e);
            throw new IllegalArgumentException(e);
        } catch (KeyStoreException e) {
            log.warn("Failed to load key {}", keyFile, e);
            throw new IllegalArgumentException(e);
        } catch (NoSuchAlgorithmException | UnrecoverableKeyException e) {
            log.warn("Failed to update key Manager", e);
            throw new IllegalArgumentException(e);
        }
        executor.scheduleWithFixedDelay(() -> updateKeyManagerSafely(), refreshDurationSec, refreshDurationSec,
                TimeUnit.SECONDS);
    }

    public void updateKeyManagerSafely() {
        try {
            updateKeyManager();
        } catch (Exception e) {
            log.warn("Failed to update key Manager for {}, {}", certFile.getFileName(), keyFile.getFileName(), e);
        }
    }

    public void updateKeyManager()
            throws CertificateException, KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException {
        if (keyManager != null && !certFile.checkAndRefresh() && !keyFile.checkAndRefresh()) {
            return;
        }
        log.info("refreshing key manager for {} {}", certFile.getFileName(), keyFile.getFileName());
        X509Certificate certificate;
        PrivateKey privateKey = null;
        KeyStore keyStore;
        try (InputStream publicCertStream = new FileInputStream(certFile.getFileName());
                InputStream privateKeyStream = new FileInputStream(keyFile.getFileName())) {
            final CertificateFactory cf = CertificateFactory.getInstance("X.509");
            certificate = (X509Certificate) cf.generateCertificate(publicCertStream);
            keyStore = KeyStore.getInstance("JKS");
            String alias = certificate.getSubjectX500Principal().getName();
            privateKey = SecurityUtility.loadPrivateKeyFromPemFile(keyFile.getFileName());
            keyStore.load(null);
            keyStore.setKeyEntry(alias, privateKey, KEYSTORE_PASSWORD, new X509Certificate[] { certificate });
        } catch (IOException | KeyManagementException e) {
            throw new IllegalArgumentException(e);
        }

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory
                .getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEYSTORE_PASSWORD);
        this.keyManager = (X509ExtendedKeyManager) keyManagerFactory.getKeyManagers()[0];
    }

    @Override
    public String[] getClientAliases(String s, Principal[] principals) {
        return keyManager.getClientAliases(s, principals);
    }

    @Override
    public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
        return keyManager.chooseClientAlias(strings, principals, socket);
    }

    @Override
    public String[] getServerAliases(String s, Principal[] principals) {
        return keyManager.getServerAliases(s, principals);
    }

    @Override
    public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
        return keyManager.chooseServerAlias(s, principals, socket);
    }

    @Override
    public X509Certificate[] getCertificateChain(String s) {
        return keyManager.getCertificateChain(s);
    }

    @Override
    public PrivateKey getPrivateKey(String s) {
        return keyManager.getPrivateKey(s);
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
        return keyManager.chooseEngineClientAlias(keyType, issuers, engine);
    }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
        return keyManager.chooseEngineServerAlias(keyType, issuers, engine);
    }

}
