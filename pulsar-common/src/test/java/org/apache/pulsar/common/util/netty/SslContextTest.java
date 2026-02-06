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
package org.apache.pulsar.common.util.netty;

import static org.testng.Assert.assertThrows;
import com.google.common.io.Resources;
import io.netty.handler.ssl.SslProvider;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.SSLException;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SslContextTest {
    static final String BROKER_KEY_STORE_PATH =
            Resources.getResource("certificate-authority/jks/broker.keystore.jks").getPath();
    static final String BROKER_TRUST_STORE_PATH =
            Resources.getResource("certificate-authority/jks/broker.truststore.jks").getPath();
    static final String KEY_STORE_TYPE = "JKS";
    static final String KEY_STORE_PASSWORD = "111111";

    static final String CA_CERT_PATH = Resources.getResource("certificate-authority/certs/ca.cert.pem").getPath();
    static final String BROKER_CERT_PATH =
            Resources.getResource("certificate-authority/server-keys/broker.cert.pem").getPath();
    static final String BROKER_KEY_PATH =
            Resources.getResource("certificate-authority/server-keys/broker.key-pk8.pem").getPath();

    @DataProvider(name = "caCertSslContextDataProvider")
    public static Object[][] getSslContextDataProvider() {
        Set<String> ciphers = new HashSet<>();
        ciphers.add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");
        ciphers.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        ciphers.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        ciphers.add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
        ciphers.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

        // Note: OPENSSL doesn't support these ciphers.
        return new Object[][]{
                new Object[]{SslProvider.JDK, ciphers},
                new Object[]{SslProvider.JDK, null},

                new Object[]{SslProvider.OPENSSL, ciphers},
                new Object[]{SslProvider.OPENSSL, null},

                new Object[]{null, ciphers},
                new Object[]{null, null},
        };
    }

    @DataProvider(name = "cipherDataProvider")
    public static Object[] getCipher() {
        Set<String> cipher = new HashSet<>();
        cipher.add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");
        cipher.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        cipher.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        cipher.add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
        cipher.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

        return new Object[]{null, cipher};
    }

    @Test(dataProvider = "cipherDataProvider")
    public void testServerKeyStoreSSLContext(Set<String> cipher) throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsEnabledWithKeystore(true)
                .tlsKeyStoreType(KEY_STORE_TYPE)
                .tlsKeyStorePath(BROKER_KEY_STORE_PATH)
                .tlsKeyStorePassword(KEY_STORE_PASSWORD)
                .allowInsecureConnection(false)
                .tlsTrustStoreType(KEY_STORE_TYPE)
                .tlsTrustStorePath(BROKER_TRUST_STORE_PATH)
                .tlsTrustStorePassword(KEY_STORE_PASSWORD)
                .requireTrustedClientCertOnConnect(true)
                .tlsCiphers(cipher)
                .build();
        try (PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory()) {
            pulsarSslFactory.initialize(pulsarSslConfiguration);
            pulsarSslFactory.createInternalSslContext();
        }
    }

    private static class ClientAuthenticationData implements AuthenticationDataProvider {
        @Override
        public KeyStoreParams getTlsKeyStoreParams() {
            return null;
        }
    }

    @Test(dataProvider = "cipherDataProvider")
    public void testClientKeyStoreSSLContext(Set<String> cipher) throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .allowInsecureConnection(false)
                .tlsEnabledWithKeystore(true)
                .tlsTrustStoreType(KEY_STORE_TYPE)
                .tlsTrustStorePath(BROKER_TRUST_STORE_PATH)
                .tlsTrustStorePassword(KEY_STORE_PASSWORD)
                .tlsCiphers(cipher)
                .authData(new ClientAuthenticationData())
                .build();
        try (PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory()) {
            pulsarSslFactory.initialize(pulsarSslConfiguration);
            pulsarSslFactory.createInternalSslContext();
        }
    }

    @Test(dataProvider = "caCertSslContextDataProvider")
    public void testServerCaCertSslContextWithSslProvider(SslProvider sslProvider, Set<String> ciphers)
            throws Exception {
        try (PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory()) {
            PulsarSslConfiguration.PulsarSslConfigurationBuilder builder = PulsarSslConfiguration.builder()
                    .tlsTrustCertsFilePath(CA_CERT_PATH)
                    .tlsCertificateFilePath(BROKER_CERT_PATH)
                    .tlsKeyFilePath(BROKER_KEY_PATH)
                    .tlsCiphers(ciphers)
                    .requireTrustedClientCertOnConnect(true);
            if (sslProvider != null) {
                builder.tlsProvider(sslProvider.name());
            }
            PulsarSslConfiguration pulsarSslConfiguration = builder.build();
            pulsarSslFactory.initialize(pulsarSslConfiguration);

            if (ciphers != null) {
                if (sslProvider == null || sslProvider == SslProvider.OPENSSL) {
                    assertThrows(SSLException.class, pulsarSslFactory::createInternalSslContext);
                    return;
                }
            }
            pulsarSslFactory.createInternalSslContext();
        }
    }

    @Test(dataProvider = "caCertSslContextDataProvider")
    public void testClientCaCertSslContextWithSslProvider(SslProvider sslProvider, Set<String> ciphers)
            throws Exception {
        try (PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory()) {
            PulsarSslConfiguration.PulsarSslConfigurationBuilder builder = PulsarSslConfiguration.builder()
                    .allowInsecureConnection(true)
                    .tlsTrustCertsFilePath(CA_CERT_PATH)
                    .tlsCiphers(ciphers);
            if (sslProvider != null) {
                builder.tlsProvider(sslProvider.name());
            }
            PulsarSslConfiguration pulsarSslConfiguration = builder.build();
            pulsarSslFactory.initialize(pulsarSslConfiguration);
            if (ciphers != null) {
                if (sslProvider == null || sslProvider == SslProvider.OPENSSL) {
                    assertThrows(SSLException.class, pulsarSslFactory::createInternalSslContext);
                    return;
                }
            }
            pulsarSslFactory.createInternalSslContext();
        }
    }
}
