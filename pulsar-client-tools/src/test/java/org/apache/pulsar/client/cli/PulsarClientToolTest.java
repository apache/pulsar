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
package org.apache.pulsar.client.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import java.util.Properties;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.tls.TlsPolicy;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests the client.conf → V5 {@link TlsPolicy} translation in
 * {@link PulsarClientTool#applyTlsPolicy}.
 */
public class PulsarClientToolTest {

    private static TlsPolicy capturePolicy(PulsarClientTool tool, String serviceUrl, Properties properties) {
        PulsarClientBuilder builder = mock(PulsarClientBuilder.class);
        tool.applyTlsPolicy(builder, serviceUrl, properties);
        ArgumentCaptor<TlsPolicy> captor = ArgumentCaptor.forClass(TlsPolicy.class);
        verify(builder).tlsPolicy(captor.capture());
        return captor.getValue();
    }

    private static TlsPolicy capturePolicy(String serviceUrl, Properties properties) {
        return capturePolicy(new PulsarClientTool(properties), serviceUrl, properties);
    }

    @Test
    public void testHostnameVerificationDefaultsOnWhenConfKeyAbsent() {
        // PIP-478 secure default: an absent tlsEnableHostnameVerification key must not disable
        // the TlsPolicy builder's verification-ON default.
        TlsPolicy policy = capturePolicy("pulsar+ssl://localhost:6651", new Properties());
        assertThat(policy.enableHostnameVerification()).isTrue();
        assertThat(policy.allowInsecureConnection()).isFalse();
        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.PEM);
    }

    @DataProvider(name = "hostnameVerificationValues")
    public Object[][] hostnameVerificationValues() {
        return new Object[][] {{"false", false}, {"true", true}};
    }

    @Test(dataProvider = "hostnameVerificationValues")
    public void testExplicitHostnameVerificationValueWins(String confValue, boolean expected) {
        Properties properties = new Properties();
        properties.setProperty("useTls", "true");
        properties.setProperty("tlsEnableHostnameVerification", confValue);
        TlsPolicy policy = capturePolicy("pulsar://localhost:6650", properties);
        assertThat(policy.enableHostnameVerification()).isEqualTo(expected);
    }

    @Test
    public void testNoTlsPolicyForPlaintextUrl() {
        PulsarClientBuilder builder = mock(PulsarClientBuilder.class);
        Properties properties = new Properties();
        new PulsarClientTool(properties).applyTlsPolicy(builder, "pulsar://localhost:6650", properties);
        verify(builder, never()).tlsPolicy(any());
    }

    @Test
    public void testPemBranchUsesConfCertAndKeyPaths() {
        Properties properties = new Properties();
        properties.setProperty("useTls", "true");
        properties.setProperty("tlsCertificateFilePath", "/path/cert.pem");
        properties.setProperty("tlsKeyFilePath", "/path/key.pem");
        PulsarClientTool tool = new PulsarClientTool(properties);
        tool.rootParams.tlsTrustCertsFilePath = "/path/ca.pem";
        TlsPolicy policy = capturePolicy(tool, "pulsar://localhost:6650", properties);
        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.PEM);
        assertThat(policy.trustCertsFilePath()).isEqualTo("/path/ca.pem");
        assertThat(policy.certificateFilePath()).isEqualTo("/path/cert.pem");
        assertThat(policy.keyFilePath()).isEqualTo("/path/key.pem");
    }

    @Test
    public void testKeyStoreTlsTranslatesToKeystorePolicy() {
        Properties properties = new Properties();
        properties.setProperty("useTls", "true");
        properties.setProperty("useKeyStoreTls", "true");
        properties.setProperty("tlsTrustStorePath", "/path/truststore.p12");
        properties.setProperty("tlsTrustStorePassword", "trust-pw");
        properties.setProperty("tlsTrustStoreType", "PKCS12");
        properties.setProperty("tlsKeyStorePath", "/path/keystore.p12");
        properties.setProperty("tlsKeyStorePassword", "key-pw");
        properties.setProperty("tlsKeyStoreType", "PKCS12");
        properties.setProperty("tlsAllowInsecureConnection", "true");
        properties.setProperty("tlsEnableHostnameVerification", "false");
        TlsPolicy policy = capturePolicy("pulsar://localhost:6650", properties);
        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.KEYSTORE);
        assertThat(policy.trustStorePath()).isEqualTo("/path/truststore.p12");
        assertThat(policy.trustStorePassword()).isEqualTo("trust-pw");
        assertThat(policy.trustStoreType()).isEqualTo("PKCS12");
        assertThat(policy.keyStorePath()).isEqualTo("/path/keystore.p12");
        assertThat(policy.keyStorePassword()).isEqualTo("key-pw");
        assertThat(policy.keyStoreType()).isEqualTo("PKCS12");
        assertThat(policy.allowInsecureConnection()).isTrue();
        assertThat(policy.enableHostnameVerification()).isFalse();
    }

    @Test
    public void testKeyStoreTypesDefaultToJksLikeV4() {
        // v4 client.conf defaulted both store types to JKS when the keys were absent; the
        // translation must preserve that (TlsPolicy's null means the JDK default type instead).
        Properties properties = new Properties();
        properties.setProperty("useTls", "true");
        properties.setProperty("useKeyStoreTls", "true");
        properties.setProperty("tlsTrustStorePath", "/path/truststore.jks");
        properties.setProperty("tlsTrustStorePassword", "trust-pw");
        TlsPolicy policy = capturePolicy("pulsar://localhost:6650", properties);
        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.KEYSTORE);
        assertThat(policy.trustStoreType()).isEqualTo("JKS");
        assertThat(policy.keyStoreType()).isEqualTo("JKS");
        assertThat(policy.enableHostnameVerification()).isTrue();
    }

    @Test
    public void testJsseProviderConfKeyIsHonored() {
        // A FIPS/provider-specific endpoint relies on the jsseProvider conf key reaching the
        // typed policy; without it the CLI would silently use the platform provider.
        Properties properties = new Properties();
        properties.setProperty("useTls", "true");
        properties.setProperty("jsseProvider", "BCJSSE");
        TlsPolicy policy = capturePolicy("pulsar://localhost:6650", properties);
        assertThat(policy.jsseProvider()).isEqualTo("BCJSSE");
    }

    @Test
    public void testJsseProviderConfKeyIsHonoredForKeystoreFormat() {
        // jsseProvider is format-independent: it must reach the policy in the KEYSTORE branch too.
        Properties properties = new Properties();
        properties.setProperty("useTls", "true");
        properties.setProperty("useKeyStoreTls", "true");
        properties.setProperty("tlsTrustStorePath", "/path/truststore.p12");
        properties.setProperty("jsseProvider", "BCJSSE");
        TlsPolicy policy = capturePolicy("pulsar://localhost:6650", properties);
        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.KEYSTORE);
        assertThat(policy.jsseProvider()).isEqualTo("BCJSSE");
    }
}
