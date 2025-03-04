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
import java.nio.file.Paths;
import java.util.HashSet;
import java.net.URISyntaxException;
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
    final static String brokerKeyStorePath;
    static {
        try {
            brokerKeyStorePath = Paths.get(Resources.getResource("certificate-authority/jks/broker.keystore.jks").toURI()).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    final static String brokerTrustStorePath;
    static {
        try {
            brokerTrustStorePath = Paths.get(Resources.getResource("certificate-authority/jks/broker.truststore.jks").toURI()).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    final static String keyStoreType = "JKS";
    final static String keyStorePassword = "111111";

    final static String caCertPath;
    static {
        try {
            caCertPath = Paths.get(Resources.getResource("certificate-authority/certs/ca.cert.pem").toURI()).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    final static String brokerCertPath;
    static {
        try {
            brokerCertPath = Paths.get(Resources.getResource("certificate-authority/server-keys/broker.cert.pem").toURI()).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    final static String brokerKeyPath;
    static {
        try {
            brokerKeyPath = Paths.get(Resources.getResource("certificate-authority/server-keys/broker.key-pk8.pem").toURI()).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @DataProvider(name = "caCertSslContextDataProvider")
    public static Object[][] getSslContextDataProvider() {
        Set<String> ciphers = new HashSet<>();
        ciphers.add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");
        ciphers.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        ciphers.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        ciphers.add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
        ciphers.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        
        return new Object[][]{
                {SslProvider.JDK, ciphers},
                {SslProvider.JDK, null},
                {SslProvider.OPENSSL, ciphers},
                {SslProvider.OPENSSL, null},
                {null, ciphers},
                {null, null},
        };
    }

    @Test(dataProvider = "caCertSslContextDataProvider")
    public void testServerCaCertSslContextWithSslProvider(SslProvider sslProvider, Set<String> ciphers) throws Exception {
        try (PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory()) {
            PulsarSslConfiguration.PulsarSslConfigurationBuilder builder = PulsarSslConfiguration.builder()
                    .tlsTrustCertsFilePath(caCertPath)
                    .tlsCertificateFilePath(brokerCertPath)
                    .tlsKeyFilePath(brokerKeyPath)
                    .tlsCiphers(ciphers)
                    .requireTrustedClientCertOnConnect(true);
            
            if (sslProvider != null) {
                builder.tlsProvider(sslProvider.name());
            }
            
            PulsarSslConfiguration pulsarSslConfiguration = builder.build();
            pulsarSslFactory.initialize(pulsarSslConfiguration);

            if (ciphers != null && (sslProvider == null || sslProvider == SslProvider.OPENSSL)) {
                assertThrows(SSLException.class, pulsarSslFactory::createInternalSslContext);
                return;
            }
            
            pulsarSslFactory.createInternalSslContext();
        }
    }
}
