/**
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

import org.apache.pulsar.PulsarClusterMetadataSetup;
import org.apache.pulsar.broker.zookeeper.ZooKeeperClientAspectJTest.ZookeeperServerTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClusterMetadataSetupTest {
    private ZookeeperServerTest localZkS;

    // test SetupClusterMetadata several times, all should be suc
    @Test
    public void testReSetupClusterMetadata() throws Exception {
        String[] args = {
            "--cluster", "testReSetupClusterMetadata-cluster",
            "--zookeeper", "127.0.0.1:" + localZkS.getZookeeperPort(),
            "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort(),
            "--web-service-url", "http://127.0.0.1:8080",
            "--web-service-url-tls", "https://127.0.0.1:8443",
            "--broker-service-url", "pulsar://127.0.0.1:6650",
            "--broker-service-url-tls","pulsar+ssl://127.0.0.1:6651"
        };
        PulsarClusterMetadataSetup.main(args);
        PulsarClusterMetadataSetup.main(args);
        PulsarClusterMetadataSetup.main(args);
    }

    @BeforeMethod
    void setup() throws Exception {
        localZkS = new ZookeeperServerTest(0);
        localZkS.start();
    }

    @AfterMethod
    void teardown() throws Exception {
        localZkS.close();
    }

}
