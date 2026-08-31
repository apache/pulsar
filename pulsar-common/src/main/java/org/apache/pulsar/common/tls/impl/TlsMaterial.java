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

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * The loaded TLS material for one {@code TlsPurpose}: the private key, its certificate chain, and the
 * trust certificates (PIP-478, internal to the default {@code FileBasedTlsFactory}).
 *
 * <p>This is a value type with structural {@code equals}/{@code hashCode} (a record). The default
 * factory uses value equality to detect real rotations: a {@code TlsMaterialSource} that re-reads its
 * files but finds identical content produces an {@code equal} instance, which suppresses spurious
 * context rebuilds and reload callbacks (a file touched without content change fires nothing).
 *
 * <p>Role-neutral: the policy flags (insecure, hostname verification, client-auth requirement) and the
 * protocols/ciphers are fixed at construction and live on the owning {@code TlsPolicy}/factory
 * settings, not here — only the crypto material rotates.
 *
 * <p><b>Multi-identity keystores.</b> PEM material describes exactly one identity, carried in
 * {@link #privateKey()}/{@link #keyCertChain()}. A keystore may hold several key entries (e.g. an RSA and
 * an EC identity, or client identities issued by different accepted CAs); those are carried in full in
 * {@link #keyEntries()} so the context builders can hand the <em>whole</em> set to a
 * {@code KeyManagerFactory} and let JSSE select an alias by the peer's requested key type / acceptable
 * issuers. For keystore material {@code privateKey}/{@code keyCertChain} mirror the first entry (so
 * {@link #hasKeyMaterial()} and the value-equality rotation check remain meaningful), but the builders
 * prefer {@code keyEntries} whenever it is non-empty.
 *
 * @param privateKey   the private key (client key for mTLS, server key on the server side), or
 *                     {@code null} when none is configured
 * @param keyCertChain the certificate chain matching {@link #privateKey()} (never {@code null})
 * @param trustCerts   the trusted CA certificates, or empty to use the platform default trust store
 * @param keyEntries   every key entry of a keystore identity (empty for PEM/single-identity material)
 */
record TlsMaterial(PrivateKey privateKey, List<X509Certificate> keyCertChain, List<X509Certificate> trustCerts,
                   List<KeyEntry> keyEntries) {

    /** The "system default" material: no key, no configured trust (resolves to the platform trust store). */
    static final TlsMaterial SYSTEM_DEFAULT = new TlsMaterial(null, List.of(), List.of());

    TlsMaterial {
        keyCertChain = keyCertChain == null ? List.of() : List.copyOf(keyCertChain);
        trustCerts = trustCerts == null ? List.of() : List.copyOf(trustCerts);
        keyEntries = keyEntries == null ? List.of() : List.copyOf(keyEntries);
    }

    /** PEM/single-identity material: one key + chain, no multi-alias keystore entries. */
    TlsMaterial(PrivateKey privateKey, List<X509Certificate> keyCertChain, List<X509Certificate> trustCerts) {
        this(privateKey, keyCertChain, trustCerts, List.of());
    }

    /** @return {@code true} when both a private key and a non-empty certificate chain are present */
    boolean hasKeyMaterial() {
        return privateKey != null && !keyCertChain.isEmpty();
    }

    /**
     * @return {@code true} when this is keystore material with at least one key entry, in which case the
     *     context builders must build the {@code KeyManager}s from the whole entry set (multi-alias
     *     selection) rather than from the single {@link #privateKey()}/{@link #keyCertChain()}
     */
    boolean hasKeyStoreEntries() {
        return !keyEntries.isEmpty();
    }

    /**
     * One key entry of a keystore identity: its alias, private key, and certificate chain. A value type so a
     * keystore that re-reads to identical content produces an {@code equal} {@link TlsMaterial} (the rotation
     * suppression), the same guarantee the single-key PEM path relies on.
     *
     * @param alias      the keystore alias (kept so a reconstructed store preserves per-alias selection)
     * @param privateKey the entry's private key
     * @param chain      the entry's certificate chain (never {@code null})
     */
    record KeyEntry(String alias, PrivateKey privateKey, List<X509Certificate> chain) {
        KeyEntry {
            chain = chain == null ? List.of() : List.copyOf(chain);
        }
    }

    /** @return the certificate chain as an array, or {@code null} when empty (Netty/JSSE "unset") */
    X509Certificate[] keyCertChainArray() {
        return keyCertChain.isEmpty() ? null : keyCertChain.toArray(new X509Certificate[0]);
    }

    /** @return the trust certificates as an array (never {@code null}; empty means platform default) */
    X509Certificate[] trustCertsArray() {
        return trustCerts.toArray(new X509Certificate[0]);
    }
}
