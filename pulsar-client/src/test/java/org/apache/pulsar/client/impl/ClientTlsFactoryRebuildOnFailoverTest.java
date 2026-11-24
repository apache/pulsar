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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.testng.annotations.Test;

/**
 * PIP-478: AutoClusterFailover mutates the client's TLS material on {@code ClientConfigurationData} at
 * runtime (updateTlsTrustCertsFilePath / updateAuthentication). The client TLS factory is composed once from
 * that material, so without a rebuild the mutation would never reach new connections — a v4-parity regression.
 * These tests assert the rebuild-not-mutate wiring: a runtime trust update rebuilds the factory and swaps it
 * into the connection pool's channel initializer so NEW connections use the updated trust, while a plaintext
 * client is unaffected. (No broker is contacted — the default file-based factory loads material lazily, so the
 * client builds and the factory is composed without connecting.)
 */
public class ClientTlsFactoryRebuildOnFailoverTest {

    @Test
    public void rebuildsAndSwapsClientTlsFactoryWhenTrustCertsUpdated() throws Exception {
        try (PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar+ssl://localhost:6651")
                .tlsTrustCertsFilePath("/pki/trust-a.pem")
                .build()) {
            PulsarTlsFactory before = client.getConfiguration().getTlsFactory();
            assertThat(before).as("TLS enabled => a client TLS factory is resolved").isNotNull();
            assertThat(client.getCnxPool().getChannelInitializerHandler().getClientTlsFactory())
                    .as("new connections start on the resolved factory").isSameAs(before);

            // AutoClusterFailover updates the trust path at runtime (as it does when switching cluster).
            client.updateTlsTrustCertsFilePath("/pki/trust-b.pem");

            PulsarTlsFactory after = client.getConfiguration().getTlsFactory();
            assertThat(after).as("the factory is rebuilt from the updated config, not the same frozen instance")
                    .isNotNull().isNotSameAs(before);
            assertThat(client.getConfiguration().getTlsTrustCertsFilePath()).isEqualTo("/pki/trust-b.pem");
            assertThat(client.getCnxPool().getChannelInitializerHandler().getClientTlsFactory())
                    .as("new connections read the rebuilt factory (existing ones keep the old one)")
                    .isSameAs(after);
        }
    }

    @Test
    public void keystoreTrustUpdateAlsoRebuildsFactory() throws Exception {
        try (PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar+ssl://localhost:6651")
                .useKeyStoreTls(true)
                .tlsTrustStorePath("/pki/trust-a.jks")
                .tlsTrustStorePassword("a")
                .build()) {
            PulsarTlsFactory before = client.getConfiguration().getTlsFactory();
            assertThat(before).isNotNull();

            client.updateTlsTrustStorePathAndPassword("/pki/trust-b.jks", "b");

            assertThat(client.getConfiguration().getTlsFactory())
                    .as("a keystore trust update also rebuilds and swaps the factory").isNotSameAs(before);
            assertThat(client.getCnxPool().getChannelInitializerHandler().getClientTlsFactory())
                    .isSameAs(client.getConfiguration().getTlsFactory());
        }
    }

    @Test
    public void plaintextClientIsNotAffected() throws Exception {
        try (PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build()) {
            assertThat(client.getConfiguration().getTlsFactory()).as("no factory on a plaintext client").isNull();
            // The update path must be a no-op rather than attempting a rebuild for a non-TLS client.
            client.updateTlsTrustCertsFilePath("/pki/trust-b.pem");
            assertThat(client.getConfiguration().getTlsFactory()).isNull();
        }
    }
}
