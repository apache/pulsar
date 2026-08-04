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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.handler.ssl.SslProvider;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.util.KeyStoreHolder;
import org.apache.pulsar.common.util.tls.JcaProviders;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.tls.TlsPolicy;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * The PIP-478 {@code jcaProvider} axis: the JCA material engines ({@code KeyStore},
 * {@code CertificateFactory}, {@code KeyFactory}) come from the pinned provider, an unset provider keeps the
 * JVM provider search order, an unsupported store type fails loudly, and the in-memory carrier keystores use
 * a generated (never empty) entry password.
 *
 * <p>The pinned provider under test is the non-FIPS BouncyCastle provider ({@code BC}), which is on this
 * module's test classpath and is the same <em>kind</em> of provider as {@code BCFIPS}: it registers
 * {@code KeyStore} (BCFKS, PKCS12), {@code CertificateFactory} (X.509) and {@code KeyFactory} services, and no
 * JSSE ones. {@code bc-fips} cannot be added here — both jars define {@code org.bouncycastle.*} and the JVM
 * rejects the mismatched signers — so a true approved-only FIPS run belongs in the separate bcfips test module.
 */
public class JcaProviderPinningTest {

    private static final String CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final char[] STORE_PW = "changeit".toCharArray();

    private Provider bc;
    private Path dir;

    @BeforeClass
    public void installProvider() {
        // Registers the BouncyCastle provider process-wide, so "BC" resolves through JcaProviders.
        bc = JcaProviders.requireBouncyCastleProvider().provider();
        assertThat(bc.getName()).isEqualTo("BC");
    }

    @BeforeMethod
    public void setUp() throws Exception {
        dir = Files.createTempDirectory("pip478-jca-");
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

    // ---------------------------------------------------------------- policy value

    @Test
    public void policyValueCarriesJcaProviderSeparatelyFromJsseProvider() {
        TlsPolicy pinned = TlsPolicy.builder().jcaProvider("BC").build();
        TlsPolicy unpinned = TlsPolicy.builder().build();
        TlsPolicy jsseOnly = TlsPolicy.builder().jsseProvider("BC").build();

        assertThat(pinned.jcaProvider()).isEqualTo("BC");
        assertThat(unpinned.jcaProvider()).isNull();
        assertThat(pinned).isNotEqualTo(unpinned);
        assertThat(pinned).isNotEqualTo(jsseOnly);
        assertThat(pinned.hashCode()).isNotEqualTo(unpinned.hashCode());
        assertThat(pinned).isEqualTo(TlsPolicy.builder().jcaProvider("BC").build());
        assertThat(pinned.toString()).contains("jcaProvider=BC");
    }

    // ---------------------------------------------------------------- keystore loading

    @Test
    public void keyStoreIsCreatedByThePinnedProvider() throws Exception {
        Path store = writePkcs12KeyStore();

        KeyStore pinned = TlsKeyStoreLoader.loadKeyStore("PKCS12", store.toString(), new String(STORE_PW), bc);
        assertThat(pinned.getProvider().getName()).isEqualTo("BC");

        KeyStore unpinned = TlsKeyStoreLoader.loadKeyStore("PKCS12", store.toString(), new String(STORE_PW), null);
        assertThat(unpinned.getProvider().getName()).as("unset provider keeps the JVM search order")
                .isNotEqualTo("BC");
    }

    @Test
    public void unsupportedStoreTypeFailsLoudlyWithTheProvidersTypes() throws Exception {
        Path store = writePkcs12KeyStore();
        // BC registers no KeyStore.JKS (nor does BCFIPS): fail loud rather than silently parsing the operator's
        // material with a different, non-validated provider.
        assertThatThrownBy(() -> TlsKeyStoreLoader.loadKeyStore("JKS", store.toString(), new String(STORE_PW), bc))
                .isInstanceOf(KeyStoreException.class)
                .hasMessageContaining("jcaProvider='BC'")
                .hasMessageContaining("JKS")
                .hasMessageContaining("BCFKS");
    }

    // ---------------------------------------------------------------- PEM material

    @Test
    public void pemMaterialIsParsedByThePinnedProvider() throws Exception {
        TlsMaterial material = new TlsMaterialSource(pemPolicy("BC")).refresh().material();

        assertThat(material.privateKey().getClass().getName())
                .as("the PrivateKey object itself is manufactured by the pinned provider")
                .contains("bouncycastle");
        assertThat(material.trustCerts().get(0).getClass().getName()).contains("bouncycastle");
        assertThat(material.keyCertChain().get(0).getClass().getName()).contains("bouncycastle");
    }

    @Test
    public void unsetProviderParsesPemMaterialExactlyAsBefore() throws Exception {
        TlsMaterialSource source = new TlsMaterialSource(pemPolicy(null));
        MaterialSource.RefreshOutcome first = source.refresh();
        TlsMaterial material = first.material();

        assertThat(first.changed()).isTrue();
        assertThat(material.privateKey().getClass().getName()).doesNotContain("bouncycastle");
        assertThat(material.trustCerts().get(0).getClass().getName()).doesNotContain("bouncycastle");
        // Rotation change detection is unaffected: a second refresh with untouched files signals no change.
        assertThat(source.refresh().changed()).isFalse();
    }

    @Test
    public void keyStoreMaterialIsParsedByThePinnedProviderAndRotationStillCompares() throws Exception {
        TlsMaterialSource source = new TlsMaterialSource(keyStorePolicy("BC"));
        TlsMaterial material = source.refresh().material();

        assertThat(material.keyEntries()).hasSize(1);
        assertThat(material.privateKey().getClass().getName()).contains("bouncycastle");
        assertThat(source.refresh().changed()).as("no file change -> stable").isFalse();
    }

    @Test
    public void brokerClientAuthMaterialIsParsedByThePinnedProvider() throws Exception {
        // The BROKER_CLIENT overlay parses the identity a proxy/broker presents to a broker; it must go
        // through the base policy's pinned provider, not silently through the JVM search order.
        //
        // This mock mirrors AuthenticationDataTls, the standard mTLS plugin: it exposes BOTH file paths AND
        // eagerly pre-parsed objects (its constructor parses the PEM through the JVM search order). A source
        // that preferred the pre-parsed objects would hand a SunRsaSign PrivateKey to a pinned deployment.
        AuthenticationDataProvider authData = mock(AuthenticationDataProvider.class);
        when(authData.hasDataForTls()).thenReturn(true);
        when(authData.getTlsCertificateFilePath()).thenReturn(BROKER_CERT);
        when(authData.getTlsPrivateKeyFilePath()).thenReturn(BROKER_KEY);
        when(authData.getTlsCertificates()).thenReturn(PemReader.loadCertificatesFromPemFile(BROKER_CERT));
        when(authData.getTlsPrivateKey()).thenReturn(PemReader.loadPrivateKeyFromPemFile(BROKER_KEY));
        assertThat(authData.getTlsPrivateKey().getClass().getName())
                .as("precondition: the plugin's own pre-parsed key is NOT from the pinned provider")
                .doesNotContain("bouncycastle");

        AuthProvidedMaterialSource overlay =
                new AuthProvidedMaterialSource(new TlsMaterialSource(pemPolicy("BC")), () -> authData);
        TlsMaterial material = overlay.refresh().material();

        assertThat(material.privateKey().getClass().getName()).contains("bouncycastle");
        assertThat(material.keyCertChain().get(0).getClass().getName()).contains("bouncycastle");
    }

    @Test
    public void brokerClientAuthMaterialKeepsPreParsedObjectsWhenNoProviderIsPinned() throws Exception {
        // The unpinned path is unchanged: the plugin's pre-parsed material still wins over its file paths.
        AuthenticationDataProvider authData = mock(AuthenticationDataProvider.class);
        when(authData.hasDataForTls()).thenReturn(true);
        when(authData.getTlsCertificateFilePath()).thenReturn(BROKER_CERT);
        when(authData.getTlsPrivateKeyFilePath()).thenReturn(BROKER_KEY);
        PrivateKey preParsed = PemReader.loadPrivateKeyFromPemFile(BROKER_KEY);
        when(authData.getTlsCertificates()).thenReturn(PemReader.loadCertificatesFromPemFile(BROKER_CERT));
        when(authData.getTlsPrivateKey()).thenReturn(preParsed);

        AuthProvidedMaterialSource overlay =
                new AuthProvidedMaterialSource(new TlsMaterialSource(pemPolicy(null)), () -> authData);

        assertThat(overlay.refresh().material().privateKey()).isSameAs(preParsed);
    }

    // ---------------------------------------------------------------- in-memory carriers

    @Test
    public void carrierKeyStoreUsesAGeneratedPasswordAndFollowsThePinnedProvider() throws Exception {
        TlsMaterial material = new TlsMaterialSource(keyStorePolicy(null)).refresh().material();

        TlsKeyStoreLoader.InMemoryKeyStore plain =
                TlsKeyStoreLoader.toInMemoryKeyStore(material.keyEntries(), null);
        assertThat(plain.password()).as("never an empty password: a FIPS PBKDF rejects one").isNotEmpty();
        assertThat(plain.keyStore().getType()).isEqualTo("PKCS12");
        assertThat(plain.keyStore().getKey("key", plain.password())).isNotNull();
        assertThatThrownBy(() -> plain.keyStore().getKey("key", new char[0]))
                .isInstanceOf(UnrecoverableKeyException.class);

        TlsKeyStoreLoader.InMemoryKeyStore pinned =
                TlsKeyStoreLoader.toInMemoryKeyStore(material.keyEntries(), bc);
        assertThat(pinned.keyStore().getProvider().getName()).isEqualTo("BC");
        assertThat(pinned.keyStore().getType()).as("prefers BC's FIPS-approved keystore format").isEqualTo("BCFKS");
        assertThat(pinned.keyStore().getKey("key", pinned.password())).isNotNull();
        assertThat(pinned.password()).as("per-build password, not a shared constant").isNotEqualTo(plain.password());
    }

    @Test
    public void keyStoreHolderUsesAGeneratedPasswordAndFollowsThePinnedProvider() throws Exception {
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(BROKER_KEY);
        X509Certificate[] chain = PemReader.loadCertificatesFromPemFile(BROKER_CERT);

        // Unpinned: the historical empty entry password is preserved, because KeyStoreHolder is public,
        // unrelocated API and existing callers read entries back with "".toCharArray().
        KeyStoreHolder plain = new KeyStoreHolder();
        plain.setPrivateKey("private", key, chain);
        assertThat(plain.getEntryPassword()).isEmpty();
        assertThat(plain.getKeyStore().getKey("private", "".toCharArray())).isNotNull();

        // Pinned: a generated password, because a FIPS PBKDF rejects an empty one.
        KeyStoreHolder pinned = new KeyStoreHolder(bc);
        pinned.setPrivateKey("private", key, chain);
        assertThat(pinned.getKeyStore().getProvider().getName()).isEqualTo("BC");
        assertThat(pinned.getEntryPassword()).isNotEmpty();
        assertThat(pinned.getKeyStore().getKey("private", pinned.getEntryPassword())).isNotNull();
        assertThat(new KeyStoreHolder(bc).getEntryPassword())
                .as("per-instance password, not a shared constant").isNotEqualTo(pinned.getEntryPassword());
    }

    // ---------------------------------------------------------------- context build

    @Test
    public void jdkContextBuildsThroughThePinnedProviderOnBothMaterialShapes() throws Exception {
        TlsMaterial keyStoreMaterial = new TlsMaterialSource(keyStorePolicy("BC")).refresh().material();
        SSLContext fromKeyStore = TlsContexts.buildJdkContext(keyStoreMaterial, keyStorePolicy("BC"));
        assertThat(fromKeyStore).isNotNull();

        TlsMaterial pemMaterial = new TlsMaterialSource(pemPolicy("BC")).refresh().material();
        SSLContext fromPem = TlsContexts.buildJdkContext(pemMaterial, pemPolicy("BC"));
        assertThat(fromPem).isNotNull();
    }

    // ---------------------------------------------------------------- helpers

    // ------------------------------------------------- the Netty engine path honors the pins too

    /**
     * The Netty builders must not hand raw key/trust material to {@code SslContextBuilder}: Netty then builds
     * the carrier {@code KeyStore} and the key/trust manager factories through the JVM provider search order,
     * which silently defeats both pins ({@code sslContextProvider} pins only the {@code SSLContext} itself).
     *
     * <p>Proven with a provider that registers <em>no</em> services at all: any code path that actually routes
     * the carrier keystore through the pin fails loudly on it, while a path that bypassed the pin would build
     * a context successfully. The material is loaded unpinned so only the context build is under test.
     */
    @Test(dataProvider = "nettyPinnedMaterial")
    public void nettyContextBuildRoutesCarrierKeyStoreThroughThePinnedJcaProvider(boolean withKey,
                                                                                  boolean withTrust)
            throws Exception {
        Provider empty = new NoServicesProvider();
        Security.addProvider(empty);
        try {
            TlsMaterial material = new TlsMaterialSource(
                    pemPolicy(null, withKey, withTrust)).refresh().material();
            TlsPolicy pinned = pemPolicy(NoServicesProvider.NAME, withKey, withTrust);

            assertThatThrownBy(() ->
                    TlsContexts.buildNettyClientContext(material, pinned, SslProvider.JDK))
                    .as("the Netty build must consult the pinned jcaProvider")
                    .hasMessageContaining("jcaProvider='" + NoServicesProvider.NAME + "'");
        } finally {
            Security.removeProvider(NoServicesProvider.NAME);
        }
    }

    @DataProvider(name = "nettyPinnedMaterial")
    public static Object[][] nettyPinnedMaterial() {
        // Key-only exercises the identity path, trust-only the trust path — each builds its own carrier.
        return new Object[][]{{true, false}, {false, true}, {true, true}};
    }

    /** A registered provider offering no services, so any pinned {@code getInstance} through it fails. */
    private static final class NoServicesProvider extends Provider {
        private static final long serialVersionUID = 1L;
        static final String NAME = "PIP478-NO-SERVICES";

        NoServicesProvider() {
            super(NAME, "1.0", "test provider registering no services");
        }
    }

    private TlsPolicy pemPolicy(String jcaProvider, boolean withKey, boolean withTrust) {
        TlsPolicy.Builder builder = TlsPolicy.builder().jcaProvider(jcaProvider);
        if (withTrust) {
            builder.trustCertsFilePath(dir.resolve("ca.pem").toString());
        }
        if (withKey) {
            builder.certificateFilePath(dir.resolve("cert.pem").toString())
                    .keyFilePath(dir.resolve("key.pem").toString());
        }
        return builder.build();
    }

    private TlsPolicy pemPolicy(String jcaProvider) {
        return TlsPolicy.builder()
                .trustCertsFilePath(dir.resolve("ca.pem").toString())
                .certificateFilePath(dir.resolve("cert.pem").toString())
                .keyFilePath(dir.resolve("key.pem").toString())
                .jcaProvider(jcaProvider)
                .build();
    }

    private TlsPolicy keyStorePolicy(String jcaProvider) throws Exception {
        Path store = writePkcs12KeyStore();
        return TlsPolicy.builder().format(TlsPolicy.Format.KEYSTORE)
                .keyStorePath(store.toString()).keyStorePassword(new String(STORE_PW)).keyStoreType("PKCS12")
                .jcaProvider(jcaProvider)
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
}
