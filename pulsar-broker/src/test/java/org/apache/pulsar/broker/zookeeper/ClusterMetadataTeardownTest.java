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
package org.apache.pulsar.broker.zookeeper;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.SortedMap;
import org.apache.pulsar.PulsarClusterMetadataSetup;
import org.apache.pulsar.PulsarClusterMetadataTeardown;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ClusterMetadataTeardownTest {

    private ClusterMetadataSetupTest.ZookeeperServerTest localZkS;

    @BeforeClass
    void setup() throws Exception {
        localZkS = new ClusterMetadataSetupTest.ZookeeperServerTest(0);
        localZkS.start();
    }

    @AfterClass
    void teardown() throws Exception {
        localZkS.close();
    }

    @AfterMethod(alwaysRun = true)
    void cleanup() {
        localZkS.clear();
    }

    @Test
    public void testSetupClusterMetadataAndTeardown() throws Exception {
        String[] args1 = {
                "--cluster", "cluster1",
                "--zookeeper", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-metadata-store-config-path", "src/test/resources/conf/zk_client_enable_sasl.conf",
                "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443",
                "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls", "pulsar+ssl://127.0.0.1:6651"
        };
        PulsarClusterMetadataSetup.main(args1);
        SortedMap<String, String> data1 = localZkS.dumpData();
        String clusterDataJson = data1.get("/admin/clusters/cluster1");
        assertNotNull(clusterDataJson);
        ClusterData clusterData = ObjectMapperFactory
                .getMapper()
                .reader()
                .readValue(clusterDataJson, ClusterData.class);
        assertEquals(clusterData.getServiceUrl(), "http://127.0.0.1:8080");
        assertEquals(clusterData.getServiceUrlTls(), "https://127.0.0.1:8443");
        assertEquals(clusterData.getBrokerServiceUrl(), "pulsar://127.0.0.1:6650");
        assertEquals(clusterData.getBrokerServiceUrlTls(), "pulsar+ssl://127.0.0.1:6651");
        assertFalse(clusterData.isBrokerClientTlsEnabled());

        String[] args2 = {
                "--cluster", "cluster1",
                "--zookeeper", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-metadata-store-config-path", "src/test/resources/conf/zk_client_enable_sasl.conf",
        };
        PulsarClusterMetadataTeardown.main(args2);
        SortedMap<String, String> data2 = localZkS.dumpData();
        assertFalse(data2.containsKey("/admin/clusters/cluster1"));
    }

    @Test
    public void testSetupMultipleClusterMetadataAndTeardown() throws Exception {
        String[] cluster1Args = {
                "--cluster", "cluster1",
                "--zookeeper", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-metadata-store-config-path", "src/test/resources/conf/zk_client_enable_sasl.conf",
                "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443",
                "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls", "pulsar+ssl://127.0.0.1:6651"
        };
        PulsarClusterMetadataSetup.main(cluster1Args);
        String[] cluster2Args = {
                "--cluster", "cluster2",
                "--zookeeper", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-metadata-store-config-path", "src/test/resources/conf/zk_client_enable_sasl.conf",
                "--web-service-url", "http://127.0.0.1:8081",
                "--web-service-url-tls", "https://127.0.0.1:8445",
                "--broker-service-url", "pulsar://127.0.0.1:6651",
                "--broker-service-url-tls", "pulsar+ssl://127.0.0.1:6652"
        };
        PulsarClusterMetadataSetup.main(cluster2Args);
        SortedMap<String, String> data1 = localZkS.dumpData();
        String clusterDataJson = data1.get("/admin/clusters/cluster1");
        assertNotNull(clusterDataJson);
        ClusterData clusterData = ObjectMapperFactory
                .getMapper()
                .reader()
                .readValue(clusterDataJson, ClusterData.class);
        assertEquals(clusterData.getServiceUrl(), "http://127.0.0.1:8080");
        assertEquals(clusterData.getServiceUrlTls(), "https://127.0.0.1:8443");
        assertEquals(clusterData.getBrokerServiceUrl(), "pulsar://127.0.0.1:6650");
        assertEquals(clusterData.getBrokerServiceUrlTls(), "pulsar+ssl://127.0.0.1:6651");
        assertFalse(clusterData.isBrokerClientTlsEnabled());

        String[] args2 = {
                "--cluster", "cluster1",
                "--zookeeper", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--configuration-metadata-store-config-path", "src/test/resources/conf/zk_client_enable_sasl.conf",
        };
        PulsarClusterMetadataTeardown.main(args2);
        SortedMap<String, String> data2 = localZkS.dumpData();
        assertFalse(data2.containsKey("/admin/clusters/cluster1"));
        assertTrue(data2.containsKey("/admin/clusters/cluster2"));

        assertTrue(data2.containsKey("/admin/policies/public"));
        assertFalse(data2.get("/admin/policies/public").contains("cluster1"));
        assertTrue(data2.get("/admin/policies/public").contains("cluster2"));

        assertTrue(data2.containsKey("/admin/policies/pulsar"));
        assertFalse(data2.get("/admin/policies/pulsar").contains("cluster1"));
        assertTrue(data2.get("/admin/policies/pulsar").contains("cluster2"));

        assertTrue(data2.containsKey("/admin/policies/public/default"));
        assertFalse(data2.get("/admin/policies/public/default").contains("cluster1"));
        assertTrue(data2.get("/admin/policies/public/default").contains("cluster2"));

        assertTrue(data2.containsKey("/admin/policies/pulsar/system"));
        assertFalse(data2.get("/admin/policies/pulsar/system").contains("cluster1"));
        assertTrue(data2.get("/admin/policies/pulsar/system").contains("cluster2"));
    }
}
