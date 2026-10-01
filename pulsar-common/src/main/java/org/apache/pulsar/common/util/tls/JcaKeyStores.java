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
package org.apache.pulsar.common.util.tls;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Creates the JCA <em>material</em> engine objects ({@link KeyStore}, {@link CertificateFactory}) either from
 * the JVM provider search order or from a pinned JCA provider — the PIP-478 {@code jcaProvider} axis.
 *
 * <p>This is deliberately a separate axis from the JSSE {@code jsseProvider} handled by
 * {@link JdkSslContexts}: {@code SSLContext}, {@code KeyManagerFactory} and {@code TrustManagerFactory} are
 * JSSE service types (a crypto-only provider such as {@code BCFIPS} registers none of them), whereas
 * {@code KeyStore}/{@code CertificateFactory}/{@code KeyFactory} are {@code java.security} (JCA) engine
 * classes that a FIPS crypto provider does register. Pinning the JCA axis is what keeps operator key material
 * parsed — and the {@code PrivateKey} objects manufactured — inside the validated module.
 *
 * <p>When no provider is pinned ({@code null}), every method here behaves exactly like the plain one-argument
 * {@code getInstance} call, i.e. today's behaviour.
 *
 * <p><b>Fail loud, never fall back.</b> When a pinned provider does not register the requested type, this
 * throws with an actionable message listing the types the provider does register, rather than silently
 * falling back to another provider: a silent fallback would void the exact property the operator pinned the
 * provider to obtain. (Contrast the {@code KeyManagerFactory}/{@code TrustManagerFactory} algorithm
 * negotiation in {@link JdkSslContexts}, where the degradation is between interchangeable algorithm names and
 * the pinned provider still supplies the {@code SSLContext}.)
 */
public final class JcaKeyStores {

    /**
     * Preferred store types for a process-local in-memory carrier keystore under a pinned JCA provider, in
     * order: {@code BCFKS} is BouncyCastle's own FIPS-approved keystore format and avoids PKCS12's
     * password-based-encryption shapes entirely; {@code PKCS12} is the portable fallback.
     */
    private static final List<String> IN_MEMORY_STORE_TYPE_PREFERENCE = List.of("BCFKS", "PKCS12");

    /** Characters of the in-memory carrier password (a URL-safe Base64 alphabet). */
    private static final char[] PASSWORD_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();

    /**
     * Length of a generated in-memory carrier password. Comfortably above the SP 800-132 minimum a FIPS
     * provider enforces on password-based key derivation, which a zero-length password cannot satisfy.
     */
    private static final int IN_MEMORY_PASSWORD_LENGTH = 32;

    // Plain SecureRandom on purpose: getInstanceStrong() can block on /dev/random, and this runs on the
    // rotation-poll thread. The password never leaves the process and protects a store that is never persisted.
    private static final SecureRandom RANDOM = new SecureRandom();

    private JcaKeyStores() {
    }

    /**
     * Create a {@link KeyStore} of the given type, from {@code jcaProvider} when one is pinned.
     *
     * @param type        the store type (e.g. {@code PKCS12}, {@code BCFKS}, {@code JKS})
     * @param jcaProvider the pinned JCA provider, or {@code null} for the JVM provider search order
     * @return an (unloaded) {@link KeyStore} instance
     * @throws KeyStoreException if the type is unavailable — including when a pinned provider does not
     *                           register it (fail loud, with the provider's registered types in the message)
     */
    public static KeyStore keyStore(String type, Provider jcaProvider) throws KeyStoreException {
        if (jcaProvider == null) {
            return KeyStore.getInstance(type);
        }
        if (jcaProvider.getService("KeyStore", type) == null) {
            throw new KeyStoreException("jcaProvider='" + jcaProvider.getName() + "' does not supply KeyStore type '"
                    + type + "'. Types this provider registers: " + registeredTypes(jcaProvider, "KeyStore")
                    + ". Set the store type to one of these (BCFKS is the FIPS-approved format), or unset "
                    + "jcaProvider.");
        }
        return KeyStore.getInstance(type, jcaProvider);
    }

    /**
     * Create an X.509 {@link CertificateFactory}, from {@code jcaProvider} when one is pinned.
     *
     * @param type        the certificate type (in practice {@code X.509})
     * @param jcaProvider the pinned JCA provider, or {@code null} for the JVM provider search order
     * @return the certificate factory
     * @throws CertificateException if the type is unavailable — including when a pinned provider does not
     *                              register it
     */
    public static CertificateFactory certificateFactory(String type, Provider jcaProvider)
            throws CertificateException {
        if (jcaProvider == null) {
            return CertificateFactory.getInstance(type);
        }
        if (jcaProvider.getService("CertificateFactory", type) == null) {
            throw new CertificateException("jcaProvider='" + jcaProvider.getName() + "' does not supply "
                    + "CertificateFactory type '" + type + "'. Types this provider registers: "
                    + registeredTypes(jcaProvider, "CertificateFactory") + ". Unset jcaProvider, or pin a provider "
                    + "that supplies X.509 certificates.");
        }
        return CertificateFactory.getInstance(type, jcaProvider);
    }

    /**
     * Choose the store type for a process-local in-memory carrier keystore: with no pinned provider the
     * caller's current default ({@code defaultType}, i.e. today's behaviour), otherwise the first
     * {@link #IN_MEMORY_STORE_TYPE_PREFERENCE preferred} type the pinned provider actually registers.
     *
     * @param jcaProvider the pinned JCA provider, or {@code null}
     * @param defaultType the type to use when no provider is pinned
     * @return the carrier store type
     * @throws KeyStoreException if the pinned provider registers none of the preferred carrier types
     */
    public static String inMemoryStoreType(Provider jcaProvider, String defaultType) throws KeyStoreException {
        if (jcaProvider == null) {
            return defaultType;
        }
        for (String candidate : IN_MEMORY_STORE_TYPE_PREFERENCE) {
            if (jcaProvider.getService("KeyStore", candidate) != null) {
                return candidate;
            }
        }
        throw new KeyStoreException("jcaProvider='" + jcaProvider.getName() + "' supplies none of the in-memory "
                + "carrier keystore types " + IN_MEMORY_STORE_TYPE_PREFERENCE + " needed to build the TLS key "
                + "managers. Types this provider registers: " + registeredTypes(jcaProvider, "KeyStore")
                + ". Unset jcaProvider, or pin a provider that supplies BCFKS or PKCS12.");
    }

    /**
     * Generate a random, per-instance password for a process-local in-memory carrier keystore.
     *
     * <p>The carrier's key entries used to be stored under an empty password. PKCS12 (and BCFKS) protect key
     * entries with a password-based KDF, and a FIPS provider in approved-only mode enforces SP 800-132
     * constraints on that KDF — a minimum password length among them — so an empty password can be rejected
     * outright at context-build time. The password is generated, used, and (by the caller) zeroed within a
     * single build; it is never persisted, logged, or compared.
     *
     * @return a freshly generated password
     */
    public static char[] newInMemoryPassword() {
        char[] password = new char[IN_MEMORY_PASSWORD_LENGTH];
        for (int i = 0; i < password.length; i++) {
            password[i] = PASSWORD_ALPHABET[RANDOM.nextInt(PASSWORD_ALPHABET.length)];
        }
        return password;
    }

    /** The algorithms/types a provider registers for a service type, sorted, for actionable error messages. */
    private static String registeredTypes(Provider provider, String serviceType) {
        Set<String> types = new TreeSet<>();
        for (Provider.Service service : provider.getServices()) {
            if (serviceType.equals(service.getType())) {
                types.add(service.getAlgorithm());
            }
        }
        return types.isEmpty() ? "(none)" : String.join(", ", types);
    }
}
