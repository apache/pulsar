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
package org.apache.pulsar.broker.tls;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.utils.ResourceUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478: every outbound leg the broker drives must carry all three {@code brokerClient*} provider pins.
 *
 * <p>The pins are a matched set, not three independent knobs: {@code brokerClientSslProvider} selects the TLS
 * engine, {@code brokerClientJsseProvider} the {@code SSLContext} provider on that engine, and
 * {@code brokerClientJcaProvider} the crypto provider that parses the key material into the objects handed to
 * that context. A leg that honours BCJSSE but parses its keystore on the JVM search order is FIPS-<em>shaped</em>
 * rather than FIPS-compliant, so dropping one of the three fails open in precisely the deployment that
 * configured them.
 *
 * <p>These are asserted on the {@link ClientConfigurationData} each leg is built from, because that is the only
 * route the pins have: {@code ClientTlsFactorySupport.clientDefaultPolicy} composes the policy from those
 * fields. The broker-side {@code DefaultBrokerTlsFactory.brokerClientPolicy} reads the {@code ServiceConfiguration}
 * keys directly, but nothing routes the broker's own {@code PulsarClient} or {@code PulsarAdmin} through it.
 *
 * <p>Real provider names are used rather than placeholders: the pins are resolved when a client builds its TLS
 * policy, and an unresolvable name fails loudly by design.
 */
@Test(groups = "broker")
public class BrokerClientProviderPinsTest extends MockedPulsarServiceBaseTest {

    private static final String SSL_PROVIDER = "JDK";
    private static final String JSSE_PROVIDER = "SunJSSE";
    private static final String JCA_PROVIDER = "SUN";

    private static final String CA_CERT_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY_FILE_PATH =
            ResourceUtils.getAbsolutePath("certificate-authority/server-keys/broker.key-pk8.pem");

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // The internal client resolves its service URL from the broker's own TLS listener, so the listener has
        // to exist for that leg to be built at all.
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setBrokerClientTrustCertsFilePath(CA_CERT_FILE_PATH);

        conf.setBrokerClientTlsEnabled(true);
        conf.setBrokerClientSslProvider(SSL_PROVIDER);
        conf.setBrokerClientJsseProvider(JSSE_PROVIDER);
        conf.setBrokerClientJcaProvider(JCA_PROVIDER);
        internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void theBrokersOwnInternalClientCarriesEveryPin() throws Exception {
        assertAllThreePins(((PulsarClientImpl) pulsar.getClient()).getConfiguration(), "the internal client");
    }

    @Test
    public void theGeoReplicationClientCarriesEveryPin() throws Exception {
        PulsarClientImpl replicationClient = (PulsarClientImpl) pulsar.getBrokerService()
                .getReplicationClient("peer-cluster", Optional.of(peerCluster()));

        assertAllThreePins(replicationClient.getConfiguration(), "the geo-replication client");
    }

    @Test
    public void theCrossClusterAdminCarriesEveryPin() throws Exception {
        PulsarAdminImpl clusterAdmin = (PulsarAdminImpl) pulsar.getBrokerService()
                .getClusterPulsarAdmin("peer-cluster", Optional.of(peerCluster()));

        assertAllThreePins(clusterAdmin.getClientConfigData(), "the cross-cluster admin");
    }

    private static void assertAllThreePins(ClientConfigurationData conf, String leg) {
        assertThat(conf.getSslProvider()).as("%s must carry the TLS engine pin", leg).isEqualTo(SSL_PROVIDER);
        assertThat(conf.getJsseProvider()).as("%s must carry the JSSE pin", leg).isEqualTo(JSSE_PROVIDER);
        assertThat(conf.getJcaProvider())
                .as("%s must carry the JCA pin: without it the leg parses key material on the JVM search "
                        + "order while the operator believes the pair is pinned", leg)
                .isEqualTo(JCA_PROVIDER);
    }

    private static ClusterData peerCluster() {
        return ClusterData.builder()
                .serviceUrl("http://peer-cluster:8080")
                .brokerServiceUrl("pulsar://peer-cluster:6650")
                .serviceUrlTls("https://peer-cluster:8443")
                .brokerServiceUrlTls("pulsar+ssl://peer-cluster:6651")
                .brokerClientTlsEnabled(true)
                .build();
    }
}
