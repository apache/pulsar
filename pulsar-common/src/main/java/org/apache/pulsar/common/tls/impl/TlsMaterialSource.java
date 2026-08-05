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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.tls.JcaProviders;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.tls.TlsPolicy;

/**
 * Loads, watches and caches ONE material set (the crypto for a single {@code TlsPurpose}) from a
 * {@link TlsPolicy} for the default {@code FileBasedTlsFactory} (PIP-478).
 *
 * <p>This collapses the old branch's {@code FileBasedClient/ServerTlsMaterialSource} split into a
 * single, role-neutral class: the difference was only the value builder and a few flags, while the
 * watch/cache machinery was duplicated verbatim.
 *
 * <p><b>Keystore-over-PEM per-field precedence.</b> For each of the three material slots (trust
 * certs, private key, key certificate chain) the keystore location wins when set, otherwise the PEM
 * location is used. PEM material is loaded via {@link PemReader}; keystores (PKCS12/JKS) are read
 * with the raw {@link KeyStore} API and an alias walk.
 *
 * <p><b>Rotation detection (fixed file-stamp baseline).</b> Change detection snapshots the modification
 * times of every configured file, and — unlike the old {@code FileModifiedTimeUpdater}-based scheme,
 * which advanced the baseline <em>before</em> the load and so never retried a failed rotation until the
 * next mtime change — commits the new baseline <strong>only after a successful load</strong>. A load
 * that throws (a half-written or invalid rotated file, the canonical incident) leaves both the baseline
 * and the last-good material untouched, so the next poll observes the same change again and retries.
 * When mtimes advanced but the loaded material is byte-for-byte {@link TlsMaterial#equals(Object)
 * equal} to the cached one (a file touched without content change), the baseline is committed but no
 * change is signalled.
 *
 * <p><b>Fail-closed trust and identity.</b> A configured keystore truststore that holds zero certificates
 * is rejected rather than loaded as an empty trust list: downstream, an empty trust list is
 * indistinguishable from "no truststore configured" and both context builders then install the platform
 * default trust manager, silently trusting every public CA. v4's {@code KeyStoreSSLContext} initialised the
 * {@code TrustManagerFactory} with the explicit store and rejected every peer. The same applies to the
 * identity axis: a keystore with no usable key entry, or a PEM certificate without its private key, fails
 * the load instead of degrading to "no identity presented", which would surface only as a much later
 * handshake failure. The PEM trust axis is deliberately NOT hardened (v4 fell back to the platform trust
 * store for an empty trust file and deployments rely on that), and a PEM key without a certificate is a
 * WARN, not a failure, for the same v4-parity reason.
 *
 * <p>Not thread-safe on its own; the owning factory serialises access under its per-source monitor.
 */
@CustomLog
final class TlsMaterialSource implements MaterialSource {

    /** Snapshot recorded for a path that is currently missing or unreadable. */
    private static final FileStamp MISSING = new FileStamp(FileTime.fromMillis(Long.MIN_VALUE), -1L, null);

    /**
     * What is compared to decide whether a watched file changed. Modification time alone misses a
     * certificate replaced twice within one filesystem mtime granularity (1s on some filesystems) or
     * restored with a preserved mtime. {@link java.nio.file.attribute.BasicFileAttributes} yields all three
     * of these in the single {@code stat} the poll was doing anyway: {@code size} catches a
     * different-length replacement, and {@code fileKey} (the inode, where the filesystem supplies one)
     * catches the write-to-temp-and-rename pattern that same-length rotations otherwise defeat.
     *
     * @param modifiedAt last modification time
     * @param size       size in bytes, or {@code -1} when unknown
     * @param fileKey    the filesystem's unique file identity, or {@code null} when it supplies none
     */
    private record FileStamp(FileTime modifiedAt, long size, Object fileKey) {
    }

    private final TlsPolicy policy;
    private final List<String> watchedPaths;
    // Resolved once: the policy's jcaProvider (KeyStore/CertificateFactory/KeyFactory axis), or null when the
    // policy pins none (the JVM provider search order). Resolving here keeps the ServiceLoader walk off the
    // per-load path and fails loudly at factory init for an unresolvable name.
    private final Provider jcaProvider;

    private Map<String, FileStamp> baseline;
    private TlsMaterial cached;

    TlsMaterialSource(TlsPolicy policy) {
        this.policy = Objects.requireNonNull(policy, "policy must not be null");
        this.watchedPaths = watchedPathsFor(policy);
        this.jcaProvider = JcaProviders.resolveNamedProvider(policy.jcaProvider());
    }

    TlsPolicy policy() {
        return policy;
    }

    /**
     * @return the resolved JCA (material) provider, or {@code null} when the policy pins none. Package-private
     *         so the {@code BROKER_CLIENT} auth overlay parses its material through the same provider.
     */
    Provider jcaProvider() {
        return jcaProvider;
    }

    /**
     * Re-stat the configured files, reloading the material when they changed since the last successful
     * load. Returns the current (possibly rebuilt) material together with whether it changed in value.
     *
     * @return the refresh outcome
     * @throws Exception if the material could not be loaded (the last-good material and baseline are
     *                   left untouched so the next call retries)
     */
    @Override
    public MaterialSource.RefreshOutcome refresh() throws Exception {
        Map<String, FileStamp> snapshot = snapshotWatchedFiles();
        if (cached != null && snapshot.equals(baseline)) {
            return new MaterialSource.RefreshOutcome(cached, false);
        }
        TlsMaterial loaded = load();
        boolean changed = cached == null || !loaded.equals(cached);
        // Commit the baseline only after a successful load (keep-last-good + retry-on-next-change).
        baseline = snapshot;
        if (changed) {
            cached = loaded;
        }
        return new MaterialSource.RefreshOutcome(cached, changed);
    }

    private TlsMaterial load() throws Exception {
        List<X509Certificate> trustCerts = loadTrustCerts();
        // Keystore identity: load the whole store and carry every key entry, so a multi-identity keystore
        // (e.g. RSA + EC) preserves JSSE alias selection at context build. The single privateKey/keyCertChain
        // mirror the first entry, keeping hasKeyMaterial() and the value-equality rotation check meaningful.
        if (StringUtils.isNotBlank(policy.keyStorePath())) {
            KeyStore keyStore = TlsKeyStoreLoader.loadKeyStore(policy.keyStoreType(), policy.keyStorePath(),
                    policy.keyStorePassword(), jcaProvider);
            List<TlsMaterial.KeyEntry> entries =
                    TlsKeyStoreLoader.extractKeyEntries(keyStore, policy.keyStorePassword());
            validateKeyStoreIdentity(entries);
            TlsMaterial.KeyEntry first = entries.get(0);
            return new TlsMaterial(first.privateKey(), first.chain(), trustCerts, entries);
        }
        validatePemIdentity();
        return new TlsMaterial(loadPemPrivateKey(), loadPemCertificateChain(), trustCerts);
    }

    /**
     * Reject a keystore that holds no usable key entry. v4 handed such a store to
     * {@code KeyManagerFactory.init}, which initialised fine but produced a key manager with no aliases — a
     * certain, undiagnosed handshake failure for a server and a silently identity-less client. Failing the
     * load is a deliberate tightening: the only deployments it can break already presented no identity.
     *
     * @param entries the key entries extracted from the configured keystore
     * @throws KeyStoreException if the keystore carries no usable key entry
     */
    private void validateKeyStoreIdentity(List<TlsMaterial.KeyEntry> entries) throws KeyStoreException {
        if (entries.isEmpty()) {
            throw new KeyStoreException("Configured keystore '" + policy.keyStorePath()
                    + "' holds no usable key entry (a private key with an X.509 certificate chain); no TLS "
                    + "identity would be presented. Fix the keystore or its password, or unset keyStorePath.");
        }
    }

    /**
     * Reject a half-configured PEM identity that would be silently dropped. A certificate without its key
     * yields {@link TlsMaterial#hasKeyMaterial()} {@code == false}, so the identity is omitted from the built
     * context and the misconfiguration only surfaces as a handshake/authentication failure much later. The
     * check is deliberately <em>asymmetric</em>: a key without a certificate is what v4 silently tolerated, so
     * it stays a WARN rather than a new startup failure. Enforced here rather than in {@code TlsPolicy.Builder}
     * so custom {@code PulsarTlsFactory} implementations that build their own policies are not constrained by
     * this default factory's requirement.
     */
    private void validatePemIdentity() {
        boolean hasCert = StringUtils.isNotBlank(policy.certificateFilePath());
        boolean hasKey = StringUtils.isNotBlank(policy.keyFilePath());
        if (hasCert && !hasKey) {
            throw new IllegalArgumentException("TlsPolicy sets certificateFilePath='" + policy.certificateFilePath()
                    + "' but leaves keyFilePath unset; a certificate without its private key yields no usable TLS "
                    + "identity. Set keyFilePath, or unset certificateFilePath.");
        }
        if (hasKey && !hasCert) {
            log.warn().attr("keyFilePath", policy.keyFilePath())
                    .log("TlsPolicy sets keyFilePath but no certificateFilePath; no TLS identity will be presented");
        }
    }

    private List<X509Certificate> loadTrustCerts() throws Exception {
        if (StringUtils.isNotBlank(policy.trustStorePath())) {
            List<X509Certificate> trustCerts = TlsKeyStoreLoader.extractTrustCerts(
                    TlsKeyStoreLoader.loadKeyStore(policy.trustStoreType(), policy.trustStorePath(),
                            policy.trustStorePassword(), jcaProvider));
            if (trustCerts.isEmpty()) {
                // An empty trust list is indistinguishable from "no truststore configured" downstream, and both
                // context builders then install the platform default trust manager — silently trusting every
                // public CA. v4 initialised the TrustManagerFactory with the explicit store and rejected every peer.
                throw new KeyStoreException("Configured truststore '" + policy.trustStorePath()
                        + "' holds no X.509 certificates; refusing to fall back to the platform default trust "
                        + "store, which would trust every public CA. Fix the truststore, or unset trustStorePath.");
            }
            return trustCerts;
        }
        if (StringUtils.isNotBlank(policy.trustCertsFilePath())) {
            X509Certificate[] certs =
                    PemReader.loadCertificatesFromPemFile(policy.trustCertsFilePath(), jcaProvider);
            return certs == null ? List.of() : List.of(certs);
        }
        return List.of();
    }

    private PrivateKey loadPemPrivateKey() throws Exception {
        if (StringUtils.isNotBlank(policy.keyFilePath())) {
            return PemReader.loadPrivateKeyFromPemFile(policy.keyFilePath(), jcaProvider);
        }
        return null;
    }

    private List<X509Certificate> loadPemCertificateChain() throws Exception {
        if (StringUtils.isNotBlank(policy.certificateFilePath())) {
            X509Certificate[] certs =
                    PemReader.loadCertificatesFromPemFile(policy.certificateFilePath(), jcaProvider);
            return certs == null ? List.of() : List.of(certs);
        }
        return List.of();
    }

    private Map<String, FileStamp> snapshotWatchedFiles() {
        Map<String, FileStamp> snapshot = new LinkedHashMap<>();
        for (String path : watchedPaths) {
            FileStamp stamp;
            try {
                BasicFileAttributes attributes =
                        Files.readAttributes(Paths.get(path), BasicFileAttributes.class);
                stamp = new FileStamp(attributes.lastModifiedTime(), attributes.size(), attributes.fileKey());
            } catch (Exception e) {
                stamp = MISSING;
            }
            snapshot.put(path, stamp);
        }
        return snapshot;
    }

    private static List<String> watchedPathsFor(TlsPolicy policy) {
        List<String> paths = new ArrayList<>(5);
        addIfPresent(paths, policy.trustCertsFilePath());
        addIfPresent(paths, policy.certificateFilePath());
        addIfPresent(paths, policy.keyFilePath());
        addIfPresent(paths, policy.keyStorePath());
        addIfPresent(paths, policy.trustStorePath());
        return List.copyOf(paths);
    }

    private static void addIfPresent(List<String> paths, String path) {
        if (StringUtils.isNotBlank(path)) {
            paths.add(path);
        }
    }
}
