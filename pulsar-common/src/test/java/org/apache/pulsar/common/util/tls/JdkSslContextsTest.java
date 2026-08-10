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

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.io.Resources;
import java.io.File;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509KeyManager;
import org.testng.annotations.Test;

/**
 * PIP-478 (FIX): {@link JdkSslContexts#createSslContextWithProvider} pins the {@code KeyManagerFactory} to the
 * resolved jsseProvider (like the {@code SSLContext} and {@code TrustManagerFactory}), while the null-provider
 * path is unchanged so an unset jsseProvider does not regress.
 */
public class JdkSslContextsTest {

    private static String resource(String name) {
        return new File(Resources.getResource(name).getPath()).getAbsolutePath();
    }

    private static final String RSA_CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    // EC identity, so a two-entry keystore holds two distinct key types (RSA + EC).
    private static final String EC_CERT = resource("certificate-authority/ec/client.cert.pem");
    private static final String EC_KEY = resource("certificate-authority/ec/client.key-pk8.pem");

    // With a client key present (cert + key non-null) setupKeyManager builds a KeyManagerFactory; FIX 5 pins it
    // to the resolved provider. SunJSSE supplies the default KMF algorithm, so the build succeeds and the whole
    // context (SSLContext + KMF + TMF) is backed by the pinned JSSE provider — the FIPS JSSE-provider (BCJSSE) intent.
    @Test
    public void pinsKeyManagerFactoryToTheProviderWithClientKey() throws Exception {
        X509Certificate[] trust = PemReader.loadCertificatesFromPemFile(RSA_CA);
        X509Certificate[] cert = PemReader.loadCertificatesFromPemFile(BROKER_CERT);
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(BROKER_KEY);
        Provider sunjsse = Security.getProvider("SunJSSE");
        assertThat(sunjsse).as("SunJSSE is installed in every JVM").isNotNull();

        SSLContext ctx = JdkSslContexts.createSslContextWithProvider(false, trust, cert, key, sunjsse);
        assertThat(ctx).isNotNull();
        assertThat(ctx.getProvider().getName())
                .as("the SSLContext (and now the KeyManagerFactory) is backed by the pinned provider")
                .isEqualTo("SunJSSE");
    }

    // Regression guard: provider == null keeps the KeyManagerFactory on the default (no-provider) form, so an
    // unset jsseProvider is unaffected by FIX 5.
    @Test
    public void nullProviderPathUnchanged() throws Exception {
        X509Certificate[] trust = PemReader.loadCertificatesFromPemFile(RSA_CA);
        X509Certificate[] cert = PemReader.loadCertificatesFromPemFile(BROKER_CERT);
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(BROKER_KEY);

        SSLContext ctx = JdkSslContexts.createSslContextWithProvider(false, trust, cert, key, null);
        assertThat(ctx).isNotNull();
    }

    // BCJSSE registers KeyManagerFactory.X.509 with X509/PKIX aliases but NOT the JDK default "SunX509"
    // (bc-java BouncyCastleJsseProvider.configure()), so the factory algorithm must be negotiated per
    // provider rather than pinned to KeyManagerFactory.getDefaultAlgorithm().
    @Test
    public void algorithmNegotiationSelectsTheAliasedFallbackForBcjsseShapedProviders() {
        Provider bcjsseShaped = new Provider("BCJSSE-shaped", "1.0", "test stub") {
            {
                put("KeyManagerFactory.X.509", "org.example.StubKeyManagerFactorySpi");
                put("Alg.Alias.KeyManagerFactory.X509", "X.509");
                put("Alg.Alias.KeyManagerFactory.PKIX", "X.509");
                put("TrustManagerFactory.PKIX", "org.example.StubTrustManagerFactorySpi");
            }
        };
        assertThat(JdkSslContexts.supportedAlgorithm(bcjsseShaped, "KeyManagerFactory", "SunX509", "PKIX"))
                .as("the JDK default SunX509 is absent; the PKIX alias resolves").isEqualTo("PKIX");
        assertThat(JdkSslContexts.supportedAlgorithm(bcjsseShaped, "TrustManagerFactory", "PKIX", "PKIX"))
                .isEqualTo("PKIX");
    }

    // A JSSE provider with no key/trust-manager services at all (Conscrypt) selects no provider algorithm;
    // the caller then falls back to the platform default factory while the SSLContext stays pinned.
    @Test
    public void algorithmNegotiationReturnsNullForProvidersWithoutTheService() {
        Provider conscryptShaped = new Provider("Conscrypt-shaped", "1.0", "test stub") { };
        assertThat(JdkSslContexts.supportedAlgorithm(conscryptShaped, "KeyManagerFactory", "SunX509", "PKIX"))
                .isNull();
        assertThat(JdkSslContexts.supportedAlgorithm(conscryptShaped, "TrustManagerFactory", "PKIX", "PKIX"))
                .isNull();
    }

    @Test
    public void algorithmNegotiationPrefersTheDefaultWhenRegistered() {
        Provider sunjsse = Security.getProvider("SunJSSE");
        assertThat(JdkSslContexts.supportedAlgorithm(sunjsse, "KeyManagerFactory",
                KeyManagerFactory.getDefaultAlgorithm(), "PKIX"))
                .isEqualTo(KeyManagerFactory.getDefaultAlgorithm());
    }

    // PIP-478 (multi-alias FIX): a KeyManagerFactory built from a keystore that holds two identities of
    // different key types (RSA + EC) must expose an alias for EACH, so JSSE can select one by the peer's
    // requested key type / acceptable issuers. The pre-fix path extracted only the first alias, which fails
    // whenever the peer requires the other identity.
    @Test
    public void keyManagerFactoryFromMultiEntryKeystoreExposesEveryKeyType() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("rsa", PemReader.loadPrivateKeyFromPemFile(BROKER_KEY), new char[0],
                PemReader.loadCertificatesFromPemFile(BROKER_CERT));
        keyStore.setKeyEntry("ec", PemReader.loadPrivateKeyFromPemFile(EC_KEY), new char[0],
                PemReader.loadCertificatesFromPemFile(EC_CERT));

        KeyManagerFactory kmf = JdkSslContexts.createKeyManagerFactory(keyStore, new char[0], null);
        X509KeyManager km = firstX509KeyManager(kmf.getKeyManagers());

        assertThat(km.getServerAliases("RSA", null)).as("RSA identity is selectable").isNotEmpty();
        assertThat(km.getServerAliases("EC", null)).as("EC identity is selectable").isNotEmpty();
        assertThat(km.getClientAliases("RSA", null)).as("RSA identity is selectable as a client").isNotEmpty();
        assertThat(km.getClientAliases("EC", null)).as("EC identity is selectable as a client").isNotEmpty();

        // The KeyManager[] overload assembles an SSLContext from the multi-alias key managers.
        SSLContext ctx = JdkSslContexts.createSslContextWithProvider(false,
                PemReader.loadCertificatesFromPemFile(RSA_CA), kmf.getKeyManagers(), null);
        assertThat(ctx).isNotNull();
    }

    private static X509KeyManager firstX509KeyManager(KeyManager[] keyManagers) {
        for (KeyManager keyManager : keyManagers) {
            if (keyManager instanceof X509KeyManager x509KeyManager) {
                return x509KeyManager;
            }
        }
        throw new AssertionError("no X509KeyManager produced");
    }
}
