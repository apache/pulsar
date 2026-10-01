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
package org.apache.pulsar.broker.admin;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import java.util.Set;
import lombok.CustomLog;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 4b: the broker's own outbound admin client (the {@code AsyncHttpConnector} behind
 * {@link org.apache.pulsar.broker.PulsarService#getAdminClient()}) on the new TLS SPI, opted in via
 * {@code brokerClientTlsFactoryClassName=default}. A targeted variant of {@link BrokerAdminClientTlsAuthTest};
 * that test exercises the legacy PIP-337 default (gate off) unmodified.
 */
@CustomLog
@Test(groups = "broker-admin")
public class BrokerAdminClientTlsFactoryTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setWebServicePortTls(Optional.of(0));
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setAuthenticationEnabled(true);
        // The broker's own admin client authenticates with the broker cert (CN=broker-localhost-SAN).
        conf.setSuperUserRoles(Set.of("broker-localhost-SAN"));
        conf.setAuthenticationProviders(
                Set.of("org.apache.pulsar.broker.authentication.AuthenticationProviderTls"));
        conf.setAuthorizationEnabled(true);
        conf.setBrokerClientTlsEnabled(true);
        conf.setTlsAllowInsecureConnection(true);
        // Opt in: route the broker's own outbound clients — including the admin client — onto
        // the PIP-478 TLS SPI. The base MockedPulsarServiceBaseTest wires the broker-client AuthenticationTls
        // material (broker cert/key + CA trust) automatically for the web-TLS + AuthenticationProviderTls case.
        conf.setBrokerClientTlsFactoryClassName("default");
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void brokerAdminClientRidesNewTlsPathAndWorksOverTls() throws Exception {
        // The broker's own admin client targets its https web service; the gate routes its AsyncHttpConnector
        // onto the new SPI.
        PulsarAdminImpl admin = (PulsarAdminImpl) pulsar.getAdminClient();
        assertThat(admin.getServiceUrl()).startsWith("https://");
        assertThat(admin.getAsyncHttpConnector().getTlsFactory())
                .as("the broker admin client should ride the PIP-478 TLS SPI when "
                        + "brokerClientTlsFactoryClassName is set")
                .isNotNull();
        // Create then read a cluster over TLS: a full write+read admin round-trip driven by the new-SPI
        // handshake (a TLS failure on either call would throw, so completion proves the factory is wired).
        admin.clusters().createCluster(configClusterName,
                ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        assertThat(admin.clusters().getClusters()).contains(configClusterName);
    }
}
