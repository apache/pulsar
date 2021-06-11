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
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.keystoretls.KeyStoreSSLContext;
/**
 * Holder for the secure key store.
 *
 * @see java.security.KeyStore
 */
public class KeyStoreHolder {

    private KeyStore keyStore = null;
    private char[] password;
    private static final char[] DEFAULT_PASSWORD = "".toCharArray();

    public KeyStoreHolder() throws KeyStoreException {
        try {
            keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyStoreException("KeyStore creation error", e);
        }
    }

    public KeyStoreHolder(String keyStoreTypeString, String keyStorePath, String keyStorePassword)
            throws KeyStoreException {
        if (StringUtils.isEmpty(keyStorePath)) {
            return;
        }
        keyStore = KeyStore
                .getInstance(StringUtils.isEmpty(keyStoreTypeString) ? KeyStoreSSLContext.DEFAULT_KEYSTORE_TYPE
                        : keyStoreTypeString);
        password = StringUtils.isNoneEmpty(keyStorePassword) ? keyStorePassword.toCharArray() : DEFAULT_PASSWORD;
        try (FileInputStream inputStream = new FileInputStream(keyStorePath)) {
            try {
                keyStore.load(inputStream, password);
            } catch (NoSuchAlgorithmException | CertificateException e) {
                throw new KeyStoreException("KeyStore creation error", e);
            }
        } catch (IOException e) {
            throw new KeyStoreException("KeyStore creation error", e);
        }
    }

    public KeyStoreHolder(PrivateKey privateKey, Certificate[] certChain) throws KeyStoreException {
        setPrivateKey("private1", privateKey, certChain);
    }

    public KeyStoreHolder(String privateKeyFilePath, String certChainFilePath) throws KeyStoreException {
        if (StringUtils.isNotBlank(privateKeyFilePath) && StringUtils.isNotBlank(certChainFilePath)) {
            try {
                PrivateKey privateKey = SecurityUtility.loadPrivateKeyFromPemFile(privateKeyFilePath);
                Certificate[] certChain = SecurityUtility.loadCertificatesFromPemFile(certChainFilePath);
                setPrivateKey("private2", privateKey, certChain);
            } catch (KeyManagementException e) {
                throw new KeyStoreException("Loading private-key and cert-chain failed", e);
            }
        }
    }

    public KeyStore getKeyStore() {
        return keyStore;
    }

    public char[] getPassword() {
        return password != null ? password : DEFAULT_PASSWORD;
    }

    public void setCertificate(String alias, Certificate certificate) throws KeyStoreException {
        try {
            keyStore.setCertificateEntry(alias, certificate);
        } catch (GeneralSecurityException e) {
            throw new KeyStoreException("Failed to set the certificate", e);
        }
    }

    public void setPrivateKey(String alias, PrivateKey privateKey, Certificate[] certChain) throws KeyStoreException {
        try {
            if (keyStore == null) {
                keyStore = KeyStore.getInstance("JKS");
            }
            keyStore.load(null, null);
            keyStore.setKeyEntry(alias, privateKey, "".toCharArray(), certChain);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyStoreException("Failed to set the private key", e);
        }
    }

}
