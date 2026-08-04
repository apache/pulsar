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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.tls.TlsPolicy;
import org.testng.annotations.Test;

/**
 * Verifies the PIP-478 server-side {@code BROKER_CLIENT} authentication-material fold at the material level:
 * when a broker-client {@code Authentication} plugin supplies TLS material, its in-memory cert/key override
 * the {@code brokerClient*} file policy (auth-cert-wins) — the flip-CI blocker — while the base file trust is
 * retained, and rotation of the auth material is detected independently of the files. The end-to-end TLS
 * handshake proof lives in {@code AuthedAdminProxyHandlerNewTlsPathTest} with real client certificates.
 */
public class AuthProvidedMaterialFoldTest {

    private static final String RSA_CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");

    private static AuthProvidedMaterialSource overlay(TlsPolicy basePolicy,
            AtomicReference<AuthenticationDataProvider> authData) {
        return new AuthProvidedMaterialSource(new TlsMaterialSource(basePolicy), authData::get);
    }

    private static AuthenticationDataProvider tlsAuthData(String certFile, String keyFile) throws Exception {
        AuthenticationDataProvider authData = mock(AuthenticationDataProvider.class);
        when(authData.hasDataForTls()).thenReturn(true);
        when(authData.getTlsCertificates()).thenReturn(PemReader.loadCertificatesFromPemFile(certFile));
        when(authData.getTlsPrivateKey()).thenReturn(PemReader.loadPrivateKeyFromPemFile(keyFile));
        return authData;
    }

    private static List<X509Certificate> certs(String certFile) throws Exception {
        return List.of(PemReader.loadCertificatesFromPemFile(certFile));
    }

    @Test
    public void authCertOverridesTheFilePolicyKeyMaterial() throws Exception {
        // Base file policy points at the BROKER cert/key; the auth plugin supplies the PROXY cert/key.
        AuthProvidedMaterialSource overlay = overlay(TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY),
                new AtomicReference<>(tlsAuthData(PROXY_CERT, PROXY_KEY)));

        TlsMaterial material = overlay.refresh().material();
        assertThat(material.hasKeyMaterial()).isTrue();
        assertThat(material.keyCertChain()).as("auth cert wins over the file-policy cert")
                .isEqualTo(certs(PROXY_CERT));
        assertThat(material.privateKey()).isEqualTo(PemReader.loadPrivateKeyFromPemFile(PROXY_KEY));
        assertThat(material.trustCerts()).as("base file trust (CA) is retained").isNotEmpty();
    }

    @Test
    public void blankFilePolicyUsesAuthKeyMaterial() throws Exception {
        // The flip-CI case: no brokerClient*FilePath cert/key, only the auth plugin supplies the identity.
        AuthProvidedMaterialSource overlay = overlay(TlsPolicy.builder().trustCertsFilePath(RSA_CA).build(),
                new AtomicReference<>(tlsAuthData(PROXY_CERT, PROXY_KEY)));

        TlsMaterial material = overlay.refresh().material();
        assertThat(material.hasKeyMaterial()).as("without the fold there would be no client cert → 401").isTrue();
        assertThat(material.keyCertChain()).isEqualTo(certs(PROXY_CERT));
    }

    @Test
    public void noAuthTlsMaterialFallsBackToTheFilePolicy() throws Exception {
        AuthenticationDataProvider noTls = mock(AuthenticationDataProvider.class);
        when(noTls.hasDataForTls()).thenReturn(false);
        AuthProvidedMaterialSource overlay = overlay(TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY),
                new AtomicReference<>(noTls));

        TlsMaterial material = overlay.refresh().material();
        assertThat(material.keyCertChain()).as("no auth TLS material → keep the file-policy cert")
                .isEqualTo(certs(BROKER_CERT));
    }

    @Test
    public void refreshDetectsAuthMaterialRotationIndependentlyOfFiles() throws Exception {
        // The base policy has no watched cert/key files, so any change must come from the auth material.
        AtomicReference<AuthenticationDataProvider> current =
                new AtomicReference<>(tlsAuthData(PROXY_CERT, PROXY_KEY));
        AuthProvidedMaterialSource overlay = overlay(TlsPolicy.builder().trustCertsFilePath(RSA_CA).build(), current);

        assertThat(overlay.refresh().changed()).as("first load counts as a change").isTrue();
        assertThat(overlay.refresh().changed()).as("same auth material → no change").isFalse();

        current.set(tlsAuthData(BROKER_CERT, BROKER_KEY));
        assertThat(overlay.refresh().changed()).as("rotated auth cert is detected").isTrue();
    }
}
