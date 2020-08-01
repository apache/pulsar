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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;

/**
 * Holder for the secure key store.
 *
 * @see java.security.KeyStore
 */
public class KeyStoreHolder {

    private KeyStore keyStore = null;

    public KeyStoreHolder() throws KeyStoreException {
        try {
            keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyStoreException("KeyStore creation error", e);
        }
    }

    public KeyStore getKeyStore() {
        return keyStore;
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
            keyStore.setKeyEntry(alias, privateKey, "".toCharArray(), certChain);
        } catch (GeneralSecurityException e) {
            throw new KeyStoreException("Failed to set the private key", e);
        }
    }

}
