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
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Optional;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class ReplicatorTlsTest extends ReplicatorTestBase {

    @Override
    @BeforeClass(timeOut = 300000)
    public void setup() throws Exception {
        config1.setBrokerClientTlsEnabled(true);
        config2.setBrokerClientTlsEnabled(true);
        config3.setBrokerClientTlsEnabled(true);
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testReplicationClient() throws Exception {
        log.info("--- Starting ReplicatorTlsTest::testReplicationClient ---");
        for (BrokerService ns : List.of(ns1, ns2, ns3)) {
            // load the client
            ns.getReplicationClient(cluster1, Optional.of(admin1.clusters().getCluster(cluster1)));
            ns.getReplicationClient(cluster2, Optional.of(admin1.clusters().getCluster(cluster2)));
            ns.getReplicationClient(cluster3, Optional.of(admin1.clusters().getCluster(cluster3)));

            // verify the client
            ns.getReplicationClients().forEach((cluster, client) -> {
                ClientConfigurationData configuration = ((PulsarClientImpl) client).getConfiguration();
                assertTrue(configuration.isUseTls());
                assertEquals(configuration.getTlsTrustCertsFilePath(), caCertFilePath);
                assertEquals(configuration.getTlsKeyFilePath(), clientKeyFilePath);
                assertEquals(configuration.getTlsCertificateFilePath(), clientCertFilePath);
            });
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorTlsTest.class);

}
