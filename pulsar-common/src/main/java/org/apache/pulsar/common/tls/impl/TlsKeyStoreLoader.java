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
package org.apache.pulsar.common.tls.impl;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.tls.JcaKeyStores;

/**
 * Loads TLS material (private key, certificate chain, trust certificates) from a PKCS12/JKS keystore for
 * the default {@code FileBasedTlsFactory} (PIP-478). Shared by the file-based {@link TlsMaterialSource}
 * and the {@code BROKER_CLIENT} authentication overlay ({@link AuthProvidedMaterialSource}) so a single
 * alias-walk implementation serves both.
 */
final class TlsKeyStoreLoader {

    /**
     * A reconstructed in-memory carrier keystore together with the generated password its key entries are
     * stored under — the password a {@code KeyManagerFactory} must be initialized with to read them.
     *
     * <p>Carrying the password as a value (rather than a shared constant read at two distant call sites)
     * makes it impossible for the store and the password to drift apart, and lets the password be random per
     * build: the entries used to be stored under an empty password, which a FIPS provider in approved-only
     * mode can reject outright because PKCS12/BCFKS protect key entries with a password-based KDF subject to
     * SP 800-132 constraints. The password never leaves the process and the caller zeroes it after use.
     *
     * @param keyStore the reconstructed, loaded keystore
     * @param password the password every key entry is stored under
     */
    record InMemoryKeyStore(KeyStore keyStore, char[] password) {
    }

    private TlsKeyStoreLoader() {
    }

    static KeyStore loadKeyStore(String type, String path, String password, Provider jcaProvider) throws Exception {
        String storeType = StringUtils.isNotBlank(type) ? type : KeyStore.getDefaultType();
        KeyStore keyStore = JcaKeyStores.keyStore(storeType, jcaProvider);
        try (InputStream input = new FileInputStream(path)) {
            keyStore.load(input, password == null ? null : password.toCharArray());
        }
        return keyStore;
    }

    static List<X509Certificate> extractTrustCerts(KeyStore trustStore) throws Exception {
        List<X509Certificate> certs = new ArrayList<>();
        Enumeration<String> aliases = trustStore.aliases();
        while (aliases.hasMoreElements()) {
            Certificate certificate = trustStore.getCertificate(aliases.nextElement());
            if (certificate instanceof X509Certificate x509) {
                certs.add(x509);
            }
        }
        return certs;
    }

    static PrivateKey extractPrivateKey(KeyStore keyStore, String password) throws Exception {
        char[] pwd = password == null ? null : password.toCharArray();
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                Key key = keyStore.getKey(alias, pwd);
                if (key instanceof PrivateKey privateKey) {
                    return privateKey;
                }
            }
        }
        return null;
    }

    /**
     * Extract <em>every</em> key entry (private key + certificate chain) of a keystore, ordered by alias for
     * a deterministic {@link TlsMaterial.KeyEntry} list (so a re-read of identical content compares equal).
     * Unlike {@link #extractPrivateKey}/{@link #extractCertificateChain}, which return only the first alias,
     * this preserves multi-identity keystores so JSSE can select an alias by the peer's requested key type /
     * acceptable issuers.
     */
    static List<TlsMaterial.KeyEntry> extractKeyEntries(KeyStore keyStore, String password) throws Exception {
        char[] pwd = password == null ? null : password.toCharArray();
        List<TlsMaterial.KeyEntry> entries = new ArrayList<>();
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (!keyStore.isKeyEntry(alias)) {
                continue;
            }
            Key key = keyStore.getKey(alias, pwd);
            if (!(key instanceof PrivateKey privateKey)) {
                continue;
            }
            Certificate[] chain = keyStore.getCertificateChain(alias);
            if (chain == null || chain.length == 0) {
                continue;
            }
            List<X509Certificate> x509Chain = new ArrayList<>(chain.length);
            for (Certificate certificate : chain) {
                if (certificate instanceof X509Certificate x509) {
                    x509Chain.add(x509);
                }
            }
            if (!x509Chain.isEmpty()) {
                entries.add(new TlsMaterial.KeyEntry(alias, privateKey, x509Chain));
            }
        }
        entries.sort(Comparator.comparing(TlsMaterial.KeyEntry::alias));
        return entries;
    }

    /**
     * Reconstruct an in-memory keystore from extracted {@link TlsMaterial.KeyEntry key entries}, each stored
     * under a freshly generated password returned alongside the store. The result feeds a
     * {@code KeyManagerFactory} at context-build time so multi-alias selection is preserved; rebuilding from
     * the value-typed entries (rather than carrying the original {@link KeyStore}, which has no value
     * equality) keeps {@link TlsMaterial}'s rotation-suppression equality intact — and the generated password
     * is deliberately per-build and outside {@link TlsMaterial}, so it never affects change detection.
     *
     * <p>The carrier store type follows the pinned JCA provider when there is one (BCFKS, else PKCS12 — the
     * types a FIPS provider registers); with no provider pinned it stays {@code PKCS12}, exactly as before.
     */
    static InMemoryKeyStore toInMemoryKeyStore(List<TlsMaterial.KeyEntry> entries, Provider jcaProvider)
            throws Exception {
        String storeType = JcaKeyStores.inMemoryStoreType(jcaProvider, "PKCS12");
        KeyStore keyStore = JcaKeyStores.keyStore(storeType, jcaProvider);
        keyStore.load(null, null);
        char[] password = JcaKeyStores.newInMemoryPassword();
        for (TlsMaterial.KeyEntry entry : entries) {
            keyStore.setKeyEntry(entry.alias(), entry.privateKey(), password,
                    entry.chain().toArray(new X509Certificate[0]));
        }
        return new InMemoryKeyStore(keyStore, password);
    }

    static List<X509Certificate> extractCertificateChain(KeyStore keyStore) throws Exception {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                Certificate[] chain = keyStore.getCertificateChain(alias);
                if (chain != null && chain.length > 0) {
                    List<X509Certificate> result = new ArrayList<>(chain.length);
                    for (Certificate certificate : chain) {
                        if (certificate instanceof X509Certificate x509) {
                            result.add(x509);
                        }
                    }
                    if (!result.isEmpty()) {
                        return result;
                    }
                }
            }
        }
        return List.of();
    }
}
