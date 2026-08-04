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
package org.apache.pulsar.common.util;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.cert.Certificate;
import java.util.Arrays;
import org.apache.pulsar.common.util.tls.JcaKeyStores;

/**
 * Holder for a process-local, in-memory key store used to carry PEM material into the JSSE
 * {@code KeyManagerFactory}/{@code TrustManagerFactory}.
 *
 * <p>When a JCA provider is pinned (PIP-478 {@code jcaProvider}), key entries are stored under a random
 * per-instance password ({@link #getEntryPassword()}) rather than an empty one: the store type is
 * password-based (PKCS12/BCFKS), and a FIPS provider in approved-only mode enforces SP 800-132 constraints on
 * the key-derivation password, which an empty password cannot satisfy. With no pinned provider the entry
 * password stays empty, exactly as before, so existing callers that read entries back with
 * {@code "".toCharArray()} keep working — {@link #getEntryPassword()} is the compatible way to read it.
 *
 * @see java.security.KeyStore
 */
public class KeyStoreHolder {

    private KeyStore keyStore = null;
    private final char[] entryPassword;

    public KeyStoreHolder() throws KeyStoreException {
        this(null);
    }

    /**
     * Create the in-memory store from a pinned JCA provider (PIP-478 {@code jcaProvider}).
     *
     * @param jcaProvider the pinned JCA provider, or {@code null} for the JVM provider search order (in which
     *                    case the JDK {@link KeyStore#getDefaultType() default store type} and the historical
     *                    empty entry password are used, exactly as before)
     * @throws KeyStoreException if the store cannot be created or the pinned provider supplies no usable type
     */
    public KeyStoreHolder(Provider jcaProvider) throws KeyStoreException {
        // Backward compatibility: only the opt-in pinned-provider path changes the entry password. This class is
        // public and unrelocated, so callers that still pass "".toCharArray() to KeyManagerFactory.init() must
        // keep working on the default path.
        this.entryPassword = jcaProvider == null ? new char[0] : JcaKeyStores.newInMemoryPassword();
        try {
            String storeType = JcaKeyStores.inMemoryStoreType(jcaProvider, KeyStore.getDefaultType());
            keyStore = JcaKeyStores.keyStore(storeType, jcaProvider);
            keyStore.load(null, null);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyStoreException("KeyStore creation error", e);
        }
    }

    public KeyStore getKeyStore() {
        return keyStore;
    }

    /**
     * @return the password this holder's key entries are stored under; a {@code KeyManagerFactory} reading
     *         them must be initialized with it. The array is owned by this holder and must not be zeroed by
     *         the caller.
     */
    public char[] getEntryPassword() {
        return Arrays.copyOf(entryPassword, entryPassword.length);
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
            keyStore.setKeyEntry(alias, privateKey, entryPassword, certChain);
        } catch (GeneralSecurityException e) {
            throw new KeyStoreException("Failed to set the private key", e);
        }
    }

}
