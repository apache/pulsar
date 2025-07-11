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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import com.google.common.io.Resources;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.OpenSslEngine;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import org.testng.annotations.Test;

public class DefaultPulsarSslFactoryTest {

    public final static String KEYSTORE_FILE_PATH =
            getAbsolutePath("certificate-authority/jks/broker.keystore.jks");
    public final static String TRUSTSTORE_FILE_PATH =
            getAbsolutePath("certificate-authority/jks/broker.truststore.jks");
    public final static String TRUSTSTORE_NO_PASSWORD_FILE_PATH =
            getAbsolutePath("certificate-authority/jks/broker.truststore.nopassword.jks");
    public final static String KEYSTORE_PW = "111111";
    public final static String TRUSTSTORE_PW = "111111";
    public final static String KEYSTORE_TYPE = "JKS";

    public final static String CA_CERT_FILE_PATH =
            getAbsolutePath("certificate-authority/certs/ca.cert.pem");
    public final static String CERT_FILE_PATH =
            getAbsolutePath("certificate-authority/server-keys/broker.cert.pem");
    public final static String KEY_FILE_PATH =
            getAbsolutePath("certificate-authority/server-keys/broker.key-pk8.pem");

    @Test
    public void sslContextCreationUsingKeystoreTest() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsEnabledWithKeystore(true)
                .tlsKeyStoreType(KEYSTORE_TYPE)
                .tlsKeyStorePath(KEYSTORE_FILE_PATH)
                .tlsKeyStorePassword(KEYSTORE_PW)
                .tlsTrustStorePath(TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(TRUSTSTORE_PW)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalSslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalNettySslContext);
    }

    @Test
    public void sslContextCreationUsingPasswordLessTruststoreTest() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsEnabledWithKeystore(true)
                .tlsKeyStoreType(KEYSTORE_TYPE)
                .tlsKeyStorePath(KEYSTORE_FILE_PATH)
                .tlsKeyStorePassword(KEYSTORE_PW)
                .tlsTrustStorePath(TRUSTSTORE_NO_PASSWORD_FILE_PATH)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalSslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalNettySslContext);
    }

    @Test
    public void sslContextCreationUsingTlsCertsTest() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsCertificateFilePath(CERT_FILE_PATH)
                .tlsKeyFilePath(KEY_FILE_PATH)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalNettySslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalSslContext);
    }

    @Test
    public void sslContextCreationUsingOnlyCACertsTest() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalNettySslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalSslContext);
    }

    @Test
    public void sslContextCreationForWebClientConnections() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsCertificateFilePath(CERT_FILE_PATH)
                .tlsKeyFilePath(KEY_FILE_PATH)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .isHttps(true)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalSslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalNettySslContext);
    }

    @Test
    public void sslContextCreationForWebServerConnectionsTest() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsCertificateFilePath(CERT_FILE_PATH)
                .tlsKeyFilePath(KEY_FILE_PATH)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .isHttps(true)
                .serverMode(true)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalSslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalNettySslContext);
    }

    @Test
    public void sslEngineCreationWithEnabledProtocolsAndCiphersForOpenSSLTest() throws Exception {
        Set<String> ciphers = new HashSet<>();
        ciphers.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        ciphers.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        Set<String> protocols = new HashSet<>();
        protocols.add("TLSv1.2");
        protocols.add("TLSv1");
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsCertificateFilePath(CERT_FILE_PATH)
                .tlsKeyFilePath(KEY_FILE_PATH)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .tlsCiphers(ciphers)
                .tlsProtocols(protocols)
                .serverMode(true)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalNettySslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalSslContext);
        SSLEngine sslEngine = pulsarSslFactory.createServerSslEngine(ByteBufAllocator.DEFAULT);
        /* Adding SSLv2Hello protocol only during expected checks as Netty adds it as part of the
        ReferenceCountedOpenSslEngine's setEnabledProtocols method. The reasoning is that OpenSSL currently has no
        way to disable this protocol.
        */
        protocols.add("SSLv2Hello");
        assertEquals(new HashSet<>(Arrays.asList(sslEngine.getEnabledProtocols())), protocols);
        assertEquals(new HashSet<>(Arrays.asList(sslEngine.getEnabledCipherSuites())), ciphers);
        assert(!sslEngine.getUseClientMode());
    }

    @Test
    public void sslEngineCreationWithEnabledProtocolsAndCiphersForWebTest() throws Exception {
        Set<String> ciphers = new HashSet<>();
        ciphers.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        ciphers.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        Set<String> protocols = new HashSet<>();
        protocols.add("TLSv1.2");
        protocols.add("TLSv1");
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsCertificateFilePath(CERT_FILE_PATH)
                .tlsKeyFilePath(KEY_FILE_PATH)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .tlsCiphers(ciphers)
                .tlsProtocols(protocols)
                .isHttps(true)
                .serverMode(true)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalSslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalNettySslContext);
        SSLEngine sslEngine = pulsarSslFactory.createServerSslEngine(ByteBufAllocator.DEFAULT);
        assertEquals(new HashSet<>(Arrays.asList(sslEngine.getEnabledProtocols())), protocols);
        assertEquals(new HashSet<>(Arrays.asList(sslEngine.getEnabledCipherSuites())), ciphers);
        assert(!sslEngine.getUseClientMode());
    }

    @Test
    public void sslContextCreationAsOpenSslTlsProvider() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsProvider("OPENSSL")
                .tlsCertificateFilePath(CERT_FILE_PATH)
                .tlsKeyFilePath(KEY_FILE_PATH)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .serverMode(true)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalNettySslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalSslContext);
        SSLEngine sslEngine = pulsarSslFactory.createServerSslEngine(ByteBufAllocator.DEFAULT);
        assert(sslEngine instanceof OpenSslEngine);
    }

    @Test
    public void sslContextCreationAsJDKTlsProvider() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsProvider("JDK")
                .tlsCertificateFilePath(CERT_FILE_PATH)
                .tlsKeyFilePath(KEY_FILE_PATH)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .serverMode(true)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalNettySslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalSslContext);
        SSLEngine sslEngine = pulsarSslFactory.createServerSslEngine(ByteBufAllocator.DEFAULT);
        assert (!(sslEngine instanceof OpenSslEngine));
    }

    @Test
    public void sslEngineMutualAuthEnabledTest() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsProvider("JDK")
                .tlsCertificateFilePath(CERT_FILE_PATH)
                .tlsKeyFilePath(KEY_FILE_PATH)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .requireTrustedClientCertOnConnect(true)
                .serverMode(true)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalNettySslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalSslContext);
        SSLEngine sslEngine = pulsarSslFactory.createServerSslEngine(ByteBufAllocator.DEFAULT);
        assert(sslEngine.getNeedClientAuth());
    }

    @Test
    public void sslEngineSniClientTest() throws Exception {
        PulsarSslConfiguration pulsarSslConfiguration = PulsarSslConfiguration.builder()
                .tlsCertificateFilePath(CERT_FILE_PATH)
                .tlsKeyFilePath(KEY_FILE_PATH)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .build();
        PulsarSslFactory pulsarSslFactory = new DefaultPulsarSslFactory();
        pulsarSslFactory.initialize(pulsarSslConfiguration);
        pulsarSslFactory.createInternalSslContext();
        assertNotNull(pulsarSslFactory.getInternalNettySslContext());
        assertThrows(RuntimeException.class, pulsarSslFactory::getInternalSslContext);
        SSLEngine sslEngine = pulsarSslFactory.createClientSslEngine(ByteBufAllocator.DEFAULT, "localhost",
                1234);
        assertEquals(sslEngine.getPeerHost(), "localhost");
        assertEquals(sslEngine.getPeerPort(), 1234);
    }



    private static String getAbsolutePath(String resourceName) {
            return new File(Resources.getResource(resourceName).getPath()).getAbsolutePath();
    }

}
