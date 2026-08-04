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

import static org.apache.pulsar.common.tls.impl.TlsTestSupport.resource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.ReferenceCountUtil;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.tls.TlsPolicy;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TlsMaterialSourceTest {

    private static final String CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");
    // EC identity, so a keystore can hold two identities of different key types (RSA broker + EC client).
    private static final String EC_CERT = resource("certificate-authority/ec/client.cert.pem");
    private static final String EC_KEY = resource("certificate-authority/ec/client.key-pk8.pem");

    private static final char[] STORE_PW = "changeit".toCharArray();

    private Path dir;

    @BeforeMethod
    public void setUp() throws Exception {
        dir = Files.createTempDirectory("pip478-src-");
        Files.copy(Paths.get(CA), dir.resolve("ca.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(BROKER_CERT), dir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(BROKER_KEY), dir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (dir != null) {
            FileUtils.deleteDirectory(dir.toFile());
        }
    }

    private TlsMaterialSource source() {
        return new TlsMaterialSource(TlsPolicy.pem(dir.resolve("ca.pem").toString(),
                dir.resolve("cert.pem").toString(), dir.resolve("key.pem").toString()));
    }

    private void bumpMtime(String name) throws Exception {
        Files.setLastModifiedTime(dir.resolve(name), FileTime.fromMillis(System.currentTimeMillis() + 5000));
    }

    @Test
    public void firstLoadIsAChangeThenStable() throws Exception {
        TlsMaterialSource source = source();
        TlsMaterialSource.RefreshOutcome first = source.refresh();
        assertThat(first.changed()).isTrue();
        assertThat(first.material().hasKeyMaterial()).isTrue();
        assertThat(first.material().trustCerts()).isNotEmpty();

        assertThat(source.refresh().changed()).as("no file change -> stable").isFalse();
    }

    @Test
    public void touchWithSameContentDoesNotSignalChange() throws Exception {
        TlsMaterialSource source = source();
        TlsMaterial initial = source.refresh().material();

        bumpMtime("cert.pem");
        TlsMaterialSource.RefreshOutcome outcome = source.refresh();
        assertThat(outcome.changed()).as("mtime advanced but content identical -> suppressed").isFalse();
        assertThat(outcome.material()).isEqualTo(initial);
    }

    @Test
    public void differentContentSignalsChange() throws Exception {
        TlsMaterialSource source = source();
        TlsMaterial initial = source.refresh().material();

        Files.copy(Paths.get(PROXY_CERT), dir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(PROXY_KEY), dir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        bumpMtime("cert.pem");
        bumpMtime("key.pem");

        TlsMaterialSource.RefreshOutcome outcome = source.refresh();
        assertThat(outcome.changed()).isTrue();
        assertThat(outcome.material()).isNotEqualTo(initial);
    }

    @Test
    public void failedLoadKeepsBaselineSoNextPollRetries() throws Exception {
        TlsMaterialSource source = source();
        TlsMaterial good = source.refresh().material();

        // Corrupt the cert: the load throws and neither the baseline nor the cached material advance.
        Files.writeString(dir.resolve("cert.pem"), "-----BEGIN CERTIFICATE-----\ngarbage\n");
        bumpMtime("cert.pem");
        assertThatThrownBy(source::refresh).isInstanceOf(Exception.class);

        // Because the baseline was NOT advanced on failure, a retry without any further mtime change
        // still attempts the load again (the fix for the old advance-before-load sharp edge).
        assertThatThrownBy(source::refresh).isInstanceOf(Exception.class);

        // A subsequent good change recovers.
        Files.copy(Paths.get(PROXY_CERT), dir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(PROXY_KEY), dir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        bumpMtime("cert.pem");
        bumpMtime("key.pem");
        TlsMaterialSource.RefreshOutcome recovered = source.refresh();
        assertThat(recovered.changed()).isTrue();
        assertThat(recovered.material()).isNotEqualTo(good);
    }

    // ---- v4-parity: separate keystore/truststore types (PIP-478) ----

    /**
     * A PKCS12 keystore paired with a JKS truststore must load — each store parsed with its OWN configured
     * type. The pre-fix policy carried a single {@code storeType} applied to all three loads, which cannot
     * express this mixed setup (and breaks outright with FIPS/BCFKS mixes or when {@code keystore.type.compat}
     * is disabled).
     */
    @Test
    public void mixedStoreTypesLoadEachWithItsOwnType() throws Exception {
        Path pkcs12KeyStore = writePkcs12KeyStore();
        Path jksTrustStore = writeJksTrustStore();

        TlsMaterial material = new TlsMaterialSource(mixedPolicy(pkcs12KeyStore, "PKCS12", jksTrustStore, "JKS"))
                .refresh().material();

        assertThat(material.hasKeyMaterial()).as("PKCS12 keystore -> key + chain loaded").isTrue();
        assertThat(material.trustCerts()).as("JKS truststore -> trust certs loaded").isNotEmpty();
    }

    /**
     * The truststore must be loaded with {@code trustStoreType()}, not the keystore type. An invalid TRUST
     * type fails the load even though the KEY type is valid — before the fix (single shared type) the good
     * key type was used for the truststore and this would NOT have failed.
     */
    @Test
    public void trustStoreTypeIsConsultedForTheTruststore() throws Exception {
        Path pkcs12KeyStore = writePkcs12KeyStore();
        Path jksTrustStore = writeJksTrustStore();
        assertThatThrownBy(() ->
                new TlsMaterialSource(mixedPolicy(pkcs12KeyStore, "PKCS12", jksTrustStore, "NOSUCHTYPE")).refresh())
                .isInstanceOf(Exception.class);
    }

    /**
     * Symmetrically, the keystore must be loaded with {@code keyStoreType()}: an invalid KEY type fails even
     * though the TRUST type is valid.
     */
    @Test
    public void keyStoreTypeIsConsultedForTheKeystore() throws Exception {
        Path pkcs12KeyStore = writePkcs12KeyStore();
        Path jksTrustStore = writeJksTrustStore();
        assertThatThrownBy(() ->
                new TlsMaterialSource(mixedPolicy(pkcs12KeyStore, "NOSUCHTYPE", jksTrustStore, "JKS")).refresh())
                .isInstanceOf(Exception.class);
    }

    /**
     * A keystore with two identities (RSA + EC) loads every key entry into {@link TlsMaterial#keyEntries()}, so
     * the context builders can preserve JSSE alias selection. The pre-fix loader kept only the first alias — the
     * v4-parity regression this exercises — and {@link TlsMaterial#hasKeyMaterial()} still mirrors the first
     * entry. A byte-for-byte re-read stays {@link TlsMaterial#equals(Object) equal} (rotation suppression).
     */
    @Test
    public void keystoreWithTwoIdentitiesCarriesEveryEntry() throws Exception {
        Path twoIdentityKeyStore = writeTwoIdentityPkcs12();
        Path jksTrustStore = writeJksTrustStore();
        TlsMaterialSource source = new TlsMaterialSource(
                mixedPolicy(twoIdentityKeyStore, "PKCS12", jksTrustStore, "JKS"));

        TlsMaterial material = source.refresh().material();
        assertThat(material.keyEntries()).as("both keystore identities are carried").hasSize(2);
        assertThat(material.keyEntries()).extracting(TlsMaterial.KeyEntry::alias)
                .as("entries are ordered by alias").containsExactly("ec", "rsa");
        assertThat(material.hasKeyStoreEntries()).isTrue();
        assertThat(material.hasKeyMaterial()).as("first entry still mirrored for back-compat").isTrue();

        // Re-reading identical content stays equal, so a touched-but-unchanged keystore suppresses a rebuild.
        assertThat(new TlsMaterialSource(mixedPolicy(twoIdentityKeyStore, "PKCS12", jksTrustStore, "JKS"))
                .refresh().material()).isEqualTo(material);
    }

    /**
     * The full production build path (the same {@code TlsContexts} calls the factory makes) constructs the Netty
     * server/client and JDK contexts from two-identity keystore material.
     */
    @Test
    public void contextsBuildFromTwoIdentityKeystoreMaterial() throws Exception {
        Path twoIdentityKeyStore = writeTwoIdentityPkcs12();
        Path jksTrustStore = writeJksTrustStore();
        TlsPolicy policy = mixedPolicy(twoIdentityKeyStore, "PKCS12", jksTrustStore, "JKS");
        TlsMaterial material = new TlsMaterialSource(policy).refresh().material();

        assertThat(TlsContexts.buildJdkContext(material, policy)).isNotNull();
        SslContext server = TlsContexts.buildNettyServerContext(material, policy, SslProvider.JDK, true);
        SslContext client = TlsContexts.buildNettyClientContext(material, policy, SslProvider.JDK);
        assertThat(server).isNotNull();
        assertThat(client).isNotNull();
        ReferenceCountUtil.release(server);
        ReferenceCountUtil.release(client);
    }

    // ---- fail-closed trust: a configured-but-empty truststore must not widen trust to the platform store ----

    /**
     * A configured JKS truststore holding zero entries must fail the load. An empty trust list is
     * indistinguishable from "no truststore configured" downstream, and both context builders then install the
     * platform default trust manager — silently trusting every public CA. v4's {@code KeyStoreSSLContext}
     * initialised the {@code TrustManagerFactory} with the explicit store and rejected every peer.
     */
    @Test
    public void emptyJksTrustStoreFailsTheLoad() throws Exception {
        assertThatThrownBy(() -> new TlsMaterialSource(trustOnlyPolicy(writeEmptyTrustStore("JKS"), "JKS")).refresh())
                .isInstanceOf(Exception.class)
                .hasMessageContaining("no X.509 certificates");
    }

    /** Same for a PKCS12 truststore — the check is on the keystore axis, not on one store type. */
    @Test
    public void emptyPkcs12TrustStoreFailsTheLoad() throws Exception {
        assertThatThrownBy(() ->
                new TlsMaterialSource(trustOnlyPolicy(writeEmptyTrustStore("PKCS12"), "PKCS12")).refresh())
                .isInstanceOf(Exception.class)
                .hasMessageContaining("no X.509 certificates");
    }

    /**
     * Rotating a good truststore to an empty one keeps the last-good material: the failed load leaves both the
     * cached material and the mtime baseline untouched, so the next refresh retries rather than silently
     * widening trust.
     */
    @Test
    public void rotationToAnEmptyTrustStoreKeepsTheLastGoodMaterial() throws Exception {
        Path keyStore = writePkcs12KeyStore();
        Path trustStore = writeJksTrustStore();
        TlsMaterialSource source = new TlsMaterialSource(mixedPolicy(keyStore, "PKCS12", trustStore, "JKS"));
        TlsMaterial good = source.refresh().material();
        assertThat(good.trustCerts()).isNotEmpty();
        FileTime goodMtime = Files.getLastModifiedTime(trustStore);

        KeyStore empty = KeyStore.getInstance("JKS");
        empty.load(null, null);
        try (OutputStream out = Files.newOutputStream(trustStore)) {
            empty.store(out, STORE_PW);
        }
        Files.setLastModifiedTime(trustStore, FileTime.fromMillis(System.currentTimeMillis() + 5000));

        assertThatThrownBy(source::refresh).isInstanceOf(Exception.class);
        // Baseline not advanced -> the same change is observed again on the next refresh (retry, still failing).
        assertThatThrownBy(source::refresh).isInstanceOf(Exception.class);

        // The property the name promises: the last-good material SURVIVED the failed rotation. Restoring the
        // pre-rotation mtime makes the source see "no change since the last successful load", so it serves its
        // cache without reloading — and what it serves must still be the good, non-empty trust material (a
        // source that had dropped its cache on the failed load would reload the now-empty store and fail).
        Files.setLastModifiedTime(trustStore, goodMtime);
        TlsMaterialSource.RefreshOutcome afterFailure = source.refresh();
        assertThat(afterFailure.changed()).isFalse();
        assertThat(afterFailure.material())
                .as("the failed rotation must leave the last-good material in place")
                .isSameAs(good);
        assertThat(afterFailure.material().trustCerts())
                .as("trust must not have been widened to the platform store by the failed rotation")
                .isEqualTo(good.trustCerts())
                .isNotEmpty();
    }

    /**
     * The PEM axis is deliberately NOT hardened: v4's {@code SecurityUtility.setupTrustCerts} fell back to the
     * platform trust store for an empty trust file, and deployments rely on that. It loads to an empty trust
     * list (WARN-logged) instead of failing.
     */
    @Test
    public void emptyPemTrustFileFallsBackToPlatformTrust() throws Exception {
        Files.writeString(dir.resolve("ca.pem"), "");
        TlsMaterial material = source().refresh().material();
        assertThat(material.trustCerts()).as("v4 parity: empty PEM trust file -> platform default trust").isEmpty();
        assertThat(material.hasKeyMaterial()).isTrue();
    }

    // ---- half-configured PEM identity ----

    /**
     * A PEM certificate without its private key yields no usable identity ({@code hasKeyMaterial()} false) and
     * would be silently dropped, surfacing only as a handshake/authentication failure. It is rejected naming
     * both fields.
     */
    @Test
    public void certificateWithoutKeyIsRejected() {
        TlsPolicy policy = TlsPolicy.builder()
                .trustCertsFilePath(dir.resolve("ca.pem").toString())
                .certificateFilePath(dir.resolve("cert.pem").toString())
                .build();
        assertThatThrownBy(() -> new TlsMaterialSource(policy).refresh())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificateFilePath")
                .hasMessageContaining("keyFilePath");
    }

    /**
     * The converse is asymmetric on purpose: v4 silently tolerated a key without a certificate, so it still
     * loads (WARN-logged) rather than becoming a new startup failure.
     */
    @Test
    public void keyWithoutCertificateStillLoads() throws Exception {
        TlsPolicy policy = TlsPolicy.builder()
                .trustCertsFilePath(dir.resolve("ca.pem").toString())
                .keyFilePath(dir.resolve("key.pem").toString())
                .build();
        TlsMaterial material = new TlsMaterialSource(policy).refresh().material();
        assertThat(material.hasKeyMaterial()).as("no chain -> no identity presented, but the load succeeds")
                .isFalse();
        assertThat(material.trustCerts()).isNotEmpty();
    }

    // ---- half-configured keystore identity (the keystore-axis counterpart of the PEM checks) ----

    /**
     * A configured keystore holding only trusted-certificate entries yields no key entry at all, so
     * {@code hasKeyMaterial()} is false and the identity is silently omitted from every built context — the
     * misconfiguration would surface only as a handshake/authentication failure. v4 handed such a store to
     * {@code KeyManagerFactory.init}, which initialised fine but produced a key manager with no aliases — a
     * certain, undiagnosed handshake failure for a server, and a silently identity-less client. Failing the load
     * is a deliberate tightening: the only deployments it can break already presented no identity.
     */
    @Test
    public void keyStoreWithoutKeyEntriesFailsTheLoad() throws Exception {
        // The JKS "truststore" (certificate entries only) deliberately pointed at as keyStorePath.
        Path certsOnly = writeJksTrustStore();
        TlsPolicy policy = TlsPolicy.builder().format(TlsPolicy.Format.KEYSTORE)
                .keyStorePath(certsOnly.toString()).keyStorePassword(new String(STORE_PW)).keyStoreType("JKS")
                .build();
        assertThatThrownBy(() -> new TlsMaterialSource(policy).refresh())
                .isInstanceOf(KeyStoreException.class)
                .hasMessageContaining(certsOnly.toString())
                .hasMessageContaining("no usable key entry");
    }

    /**
     * A keystore whose key entry is stored under a different password is unrecoverable: it must fail the load
     * rather than degrade to "no identity". This is v4 parity — {@code KeyManagerFactory.init(store, password)}
     * threw {@code UnrecoverableKeyException} for exactly this store.
     */
    @Test
    public void keyStoreWithUnrecoverableKeyFailsTheLoad() throws Exception {
        // Store password STORE_PW (so the store itself opens), entry password different (so the key does not).
        Path path = writePkcs12KeyStoreWithEntryPassword("s3cr3t".toCharArray());
        TlsPolicy policy = TlsPolicy.builder().format(TlsPolicy.Format.KEYSTORE)
                .keyStorePath(path.toString()).keyStorePassword(new String(STORE_PW)).keyStoreType("PKCS12")
                .build();
        assertThatThrownBy(() -> new TlsMaterialSource(policy).refresh())
                .as("an unrecoverable key entry must not silently degrade to 'no identity'")
                .isInstanceOf(UnrecoverableKeyException.class);
    }

    /**
     * Rotating a good keystore to one without a usable key entry keeps the last-good material: the failed load
     * leaves both the cached material and the mtime baseline untouched, so the next refresh retries.
     */
    @Test
    public void rotationToAKeylessKeyStoreKeepsTheLastGoodMaterial() throws Exception {
        Path keyStore = writePkcs12KeyStore();
        Path trustStore = writeJksTrustStore();
        TlsMaterialSource source = new TlsMaterialSource(mixedPolicy(keyStore, "PKCS12", trustStore, "JKS"));
        TlsMaterial good = source.refresh().material();
        assertThat(good.hasKeyMaterial()).isTrue();
        FileTime goodMtime = Files.getLastModifiedTime(keyStore);

        KeyStore certsOnly = KeyStore.getInstance("PKCS12");
        certsOnly.load(null, null);
        certsOnly.setCertificateEntry("ca", PemReader.loadCertificatesFromPemFile(CA)[0]);
        try (OutputStream out = Files.newOutputStream(keyStore)) {
            certsOnly.store(out, STORE_PW);
        }
        Files.setLastModifiedTime(keyStore, FileTime.fromMillis(System.currentTimeMillis() + 5000));

        assertThatThrownBy(source::refresh).isInstanceOf(KeyStoreException.class);
        // Baseline not advanced -> the same change is observed again on the next refresh (retry, still failing).
        assertThatThrownBy(source::refresh).isInstanceOf(KeyStoreException.class);

        // The property the name promises: the last-good identity SURVIVED the failed rotation. Restoring the
        // pre-rotation mtime makes the source serve its cache without reloading, and that cache must still
        // carry the key material (a source that dropped its cache on the failed load would reload the
        // keyless store and fail).
        Files.setLastModifiedTime(keyStore, goodMtime);
        TlsMaterialSource.RefreshOutcome afterFailure = source.refresh();
        assertThat(afterFailure.changed()).isFalse();
        assertThat(afterFailure.material())
                .as("the failed rotation must leave the last-good material in place")
                .isSameAs(good);
        assertThat(afterFailure.material().hasKeyMaterial())
                .as("the identity must not have been dropped by the failed rotation")
                .isTrue();
    }

    private Path writePkcs12KeyStoreWithEntryPassword(char[] entryPassword) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("key", PemReader.loadPrivateKeyFromPemFile(BROKER_KEY), entryPassword,
                PemReader.loadCertificatesFromPemFile(BROKER_CERT));
        Path path = dir.resolve("entry-pw.p12");
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, STORE_PW);
        }
        return path;
    }

    private Path writeEmptyTrustStore(String type) throws Exception {
        KeyStore ks = KeyStore.getInstance(type);
        ks.load(null, null);
        Path path = dir.resolve("empty-trust." + type.toLowerCase());
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, STORE_PW);
        }
        return path;
    }

    private static TlsPolicy trustOnlyPolicy(Path trustStore, String trustStoreType) {
        return TlsPolicy.builder().format(TlsPolicy.Format.KEYSTORE)
                .trustStorePath(trustStore.toString()).trustStorePassword(new String(STORE_PW))
                .trustStoreType(trustStoreType)
                .build();
    }

    private Path writeTwoIdentityPkcs12() throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("rsa", PemReader.loadPrivateKeyFromPemFile(BROKER_KEY), STORE_PW,
                PemReader.loadCertificatesFromPemFile(BROKER_CERT));
        ks.setKeyEntry("ec", PemReader.loadPrivateKeyFromPemFile(EC_KEY), STORE_PW,
                PemReader.loadCertificatesFromPemFile(EC_CERT));
        Path path = dir.resolve("two-identity.p12");
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, STORE_PW);
        }
        return path;
    }

    private static TlsPolicy mixedPolicy(Path keyStore, String keyStoreType, Path trustStore, String trustStoreType) {
        return TlsPolicy.builder().format(TlsPolicy.Format.KEYSTORE)
                .keyStorePath(keyStore.toString()).keyStorePassword(new String(STORE_PW)).keyStoreType(keyStoreType)
                .trustStorePath(trustStore.toString()).trustStorePassword(new String(STORE_PW))
                .trustStoreType(trustStoreType)
                .build();
    }

    private Path writePkcs12KeyStore() throws Exception {
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(BROKER_KEY);
        X509Certificate[] chain = PemReader.loadCertificatesFromPemFile(BROKER_CERT);
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("key", key, STORE_PW, chain);
        Path path = dir.resolve("key.p12");
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, STORE_PW);
        }
        return path;
    }

    private Path writeJksTrustStore() throws Exception {
        X509Certificate[] cas = PemReader.loadCertificatesFromPemFile(CA);
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null);
        for (int i = 0; i < cas.length; i++) {
            ks.setCertificateEntry("ca" + i, cas[i]);
        }
        Path path = dir.resolve("trust.jks");
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, STORE_PW);
        }
        return path;
    }
}
