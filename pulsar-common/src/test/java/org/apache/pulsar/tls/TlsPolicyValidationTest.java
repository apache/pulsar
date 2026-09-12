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
package org.apache.pulsar.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.testng.annotations.Test;

/**
 * PIP-478: {@link TlsPolicy.Builder#build()} fails loud when a configured field is inconsistent with the
 * chosen {@link TlsPolicy.Format}, rather than silently ignoring it.
 */
public class TlsPolicyValidationTest {

    @Test
    public void pemPolicyRejectsKeystoreFields() {
        assertThatThrownBy(() -> TlsPolicy.builder()
                .format(TlsPolicy.Format.PEM)
                .trustCertsFilePath("/ca.pem")
                .keyStorePath("/key.p12")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyStorePath");

        assertThatThrownBy(() -> TlsPolicy.builder()
                .format(TlsPolicy.Format.PEM)
                .keyStoreType("PKCS12")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keyStoreType");
    }

    @Test
    public void keystorePolicyRejectsPemFields() {
        assertThatThrownBy(() -> TlsPolicy.builder()
                .format(TlsPolicy.Format.KEYSTORE)
                .keyStorePath("/key.p12")
                .certificateFilePath("/cert.pem")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("certificateFilePath");
    }

    @Test
    public void consistentPoliciesBuild() {
        assertThatCode(() -> TlsPolicy.pem("/ca.pem", "/cert.pem", "/key.pem"))
                .doesNotThrowAnyException();
        assertThatCode(() -> TlsPolicy.keyStore("/trust.jks", "pw", "/key.jks", "pw", "JKS"))
                .doesNotThrowAnyException();
        // Mixed store TYPES are consistent with KEYSTORE format (only cross-format fields are rejected).
        assertThatCode(() -> TlsPolicy.builder().format(TlsPolicy.Format.KEYSTORE)
                .keyStorePath("/key.p12").keyStoreType("PKCS12")
                .trustStorePath("/trust.jks").trustStoreType("JKS")
                .build()).doesNotThrowAnyException();
        // Flag-only policies (system default / insecure) build cleanly.
        assertThat(TlsPolicy.builder().build().format()).isEqualTo(TlsPolicy.Format.PEM);
        assertThat(TlsPolicy.insecure().allowInsecureConnection()).isTrue();
    }
}
