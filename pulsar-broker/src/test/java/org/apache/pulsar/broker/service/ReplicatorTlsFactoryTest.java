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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Optional;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 4a: the broker's own outbound clients (geo-replication, cluster-internal lookup) on the new
 * TLS SPI ({@code BROKER_CLIENT}). A targeted variant of {@link ReplicatorTlsTest} that opts in via
 * {@code brokerClientTlsFactoryClassName=default}; {@link ReplicatorTlsTest} itself exercises the legacy
 * PIP-337 default (gate off) unmodified.
 */
@Test(groups = "broker-replication")
@CustomLog
public class ReplicatorTlsFactoryTest extends ReplicatorTestBase {

    @Override
    @BeforeClass(timeOut = 300000)
    public void setup() throws Exception {
        // The per-cluster replication clients already ride TLS from their ClusterData brokerClientTls* material
        // (configured by ReplicatorTestBase); opting in via brokerClientTlsFactoryClassName routes them onto the
        // PIP-478 TLS SPI. (Deliberately not setting the ServiceConfiguration brokerClientTlsEnabled flag: it is
        // unrelated to this gate — the replication path keys off ClusterData — and toggling it perturbs the
        // remote-admin harness independently of PIP-478.)
        config1.setBrokerClientTlsFactoryClassName("default");
        config2.setBrokerClientTlsFactoryClassName("default");
        config3.setBrokerClientTlsFactoryClassName("default");
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testReplicationClientOnNewTlsPath() throws Exception {
        log.info("--- Starting ReplicatorTlsFactoryTest::testReplicationClientOnNewTlsPath ---");
        for (BrokerService ns : List.of(ns1, ns2, ns3)) {
            ns.getReplicationClient(cluster1, Optional.of(admin1.clusters().getCluster(cluster1)));
            ns.getReplicationClient(cluster2, Optional.of(admin1.clusters().getCluster(cluster2)));
            ns.getReplicationClient(cluster3, Optional.of(admin1.clusters().getCluster(cluster3)));

            ns.getReplicationClients().forEach((cluster, client) -> {
                ClientConfigurationData conf = ((PulsarClientImpl) client).getConfiguration();
                assertTrue(conf.isUseTls());
                // The PIP-478 TLS factory was adopted for the broker's own outbound client.
                assertNotNull(conf.getTlsFactory(), "broker-client should ride the PIP-478 TLS SPI when "
                        + "brokerClientTlsFactoryClassName is set");
                // The broker-client material is still mapped onto the config fields (composed into the factory).
                assertEquals(conf.getTlsTrustCertsFilePath(), caCertFilePath);
                assertEquals(conf.getTlsKeyFilePath(), clientKeyFilePath);
                assertEquals(conf.getTlsCertificateFilePath(), clientCertFilePath);
            });
        }
    }

    @Test
    public void testReplicatesAcrossClustersOnNewTlsPath() throws Exception {
        log.info("--- Starting ReplicatorTlsFactoryTest::testReplicatesAcrossClustersOnNewTlsPath ---");
        // A real broker-to-broker TLS handshake on the new path: replication rides the BROKER_CLIENT
        // connection, so producing on cluster1 and consuming the replicated messages on cluster2 / cluster3
        // exercises the new-SPI TLS handshake end to end.
        final TopicName dest = TopicName.get(
                BrokerTestUtil.newUniqueName("persistent://pulsar/ns/tlsFactoryReplTopic"));

        @Cleanup
        MessageProducer producer1 = new MessageProducer(url1, dest);
        @Cleanup
        MessageConsumer consumer1 = new MessageConsumer(url1, dest);
        @Cleanup
        MessageConsumer consumer2 = new MessageConsumer(url2, dest);
        @Cleanup
        MessageConsumer consumer3 = new MessageConsumer(url3, dest);

        // Local first (confirms the topic + starts the replicators to r2/r3 over the BROKER_CLIENT connection).
        producer1.produce(2);
        consumer1.receive(2);
        // Replicated: broker-to-broker delivery over the new-SPI TLS connection.
        consumer2.receive(2, 30);
        consumer3.receive(2, 30);
    }
}
