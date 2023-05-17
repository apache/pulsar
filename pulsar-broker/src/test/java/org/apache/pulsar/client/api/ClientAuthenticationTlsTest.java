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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ClientAuthenticationTlsTest extends ProducerConsumerBase {

    private final Authentication authenticationTls =
            new AuthenticationTls(getTlsFileForClient("admin.cert"), getTlsFileForClient("admin.key-pk8"));

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();

        conf.setClusterName(configClusterName);

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);

        conf.setTlsAllowInsecureConnection(false);

        conf.setBrokerClientTlsEnabled(true);
        conf.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        conf.setBrokerClientAuthenticationParameters(
                "tlsCertFile:" + getTlsFileForClient("admin.cert")
                        + ",tlsKeyFile:" + getTlsFileForClient("admin.key-pk8"));
        conf.setBrokerClientTrustCertsFilePath(CA_CERT_FILE_PATH);
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeNewPulsarAdminBuilder(PulsarAdminBuilder pulsarAdminBuilder) {
        super.customizeNewPulsarAdminBuilder(pulsarAdminBuilder);
        pulsarAdminBuilder.authentication(authenticationTls);
    }

    @Test
    public void testAdminWithTrustCert() throws PulsarClientException, PulsarAdminException {
        @Cleanup
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(getPulsar().getWebServiceAddressTls())
                .sslProvider("JDK")
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .build();
        pulsarAdmin.clusters().getClusters();
    }

    @Test
    public void testAdminWithFull() throws PulsarClientException, PulsarAdminException {
        @Cleanup
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(getPulsar().getWebServiceAddressTls())
                .sslProvider("JDK")
                .authentication(authenticationTls)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .build();
        pulsarAdmin.clusters().getClusters();
    }

    @Test
    public void testAdminWithCertAndKey() throws PulsarClientException, PulsarAdminException {
        @Cleanup
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(getPulsar().getWebServiceAddressTls())
                .sslProvider("JDK")
                .authentication(authenticationTls)
                .build();
        PulsarAdminException adminException =
                expectThrows(PulsarAdminException.class, () -> pulsarAdmin.clusters().getClusters());
        assertTrue(adminException.getMessage().contains("PKIX path"));
    }

    @Test
    public void testAdminWithoutTls() throws PulsarClientException, PulsarAdminException {
        @Cleanup
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(getPulsar().getWebServiceAddressTls())
                .sslProvider("JDK")
                .build();
        PulsarAdminException adminException =
                expectThrows(PulsarAdminException.class, () -> pulsarAdmin.clusters().getClusters());
        assertTrue(adminException.getMessage().contains("PKIX path"));
    }

    @Test
    public void testClientWithTrustCert() throws PulsarClientException, PulsarAdminException {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsar().getBrokerServiceUrlTls())
                .sslProvider("JDK")
                .operationTimeout(3, TimeUnit.SECONDS)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .build();
        @Cleanup
        Producer<byte[]> ignored = pulsarClient.newProducer().topic(UUID.randomUUID().toString()).create();
    }

    @Test
    public void testClientWithFull() throws PulsarClientException, PulsarAdminException {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsar().getBrokerServiceUrlTls())
                .sslProvider("JDK")
                .operationTimeout(3, TimeUnit.SECONDS)
                .authentication(authenticationTls)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
                .build();
        @Cleanup
        Producer<byte[]> ignored = pulsarClient.newProducer().topic(UUID.randomUUID().toString()).create();
    }

    @Test
    public void testClientWithCertAndKey() throws PulsarClientException {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsar().getBrokerServiceUrlTls())
                .sslProvider("JDK")
                .operationTimeout(3, TimeUnit.SECONDS)
                .authentication(authenticationTls)
                .build();
        assertThrows(PulsarClientException.class,
                () -> pulsarClient.newProducer().topic(UUID.randomUUID().toString()).create());
    }

    @Test
    public void testClientWithoutTls() throws PulsarClientException, PulsarAdminException {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(getPulsar().getBrokerServiceUrlTls())
                .sslProvider("JDK")
                .operationTimeout(3, TimeUnit.SECONDS)
                .build();
        assertThrows(PulsarClientException.class,
                () -> pulsarClient.newProducer().topic(UUID.randomUUID().toString()).create());
    }
}
