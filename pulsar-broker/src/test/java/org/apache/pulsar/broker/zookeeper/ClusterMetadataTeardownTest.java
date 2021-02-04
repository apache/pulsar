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

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.PulsarClusterMetadataSetup;
import org.apache.pulsar.PulsarClusterMetadataTeardown;
import org.apache.pulsar.broker.zookeeper.ZooKeeperClientAspectJTest.ZookeeperServerTest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class ClusterMetadataTeardownTest {
    private ZookeeperServerTest localZkS;
    private ZookeeperServerTest bookieZkS;


    @Test
    public void testTeardownWithoutBkMetadataServiceUri() throws Exception {
        String zkConnection = "127.0.0.1:" + localZkS.getZookeeperPort();

        // init
        String[] initArgs = {"--cluster", "testSetupWithBkMetadata-cluster", "--zookeeper", zkConnection,
                "--configuration-store", zkConnection, "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443", "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls", "pulsar+ssl://127.0.0.1:6651"};

        PulsarClusterMetadataSetup.main(initArgs);

        ZooKeeper initZk = PulsarClusterMetadataSetup.initZk(zkConnection, 30000000);

        // expected exist
        assertNotNull(initZk.exists("/managed-ledgers", false));


        // teardown
        String[] teardownArgs = {"--cluster", "testSetupWithBkMetadata-cluster", "--zookeeper", zkConnection,
                "--configuration-store", zkConnection};

        PulsarClusterMetadataTeardown.main(teardownArgs);


        ZooKeeper teardownZk = PulsarClusterMetadataTeardown.initZk(zkConnection, 30000000);

        // expected not exist
        assertNull(teardownZk.exists("/managed-ledgers", false));

        // "/zookeeper" only
        assertEquals(1, teardownZk.getChildren("/", false).size());

    }


    @Test
    public void testTeardownWithBkMetadataServiceUri() throws Exception {
        String localZkConnection = "127.0.0.1:" + localZkS.getZookeeperPort();
        String bookieZkConnection = "127.0.0.1:" + bookieZkS.getZookeeperPort();
        String chroot = "/chroot/ledgers";

        // init
        String[] initArgs = {"--cluster", "testSetupWithBkMetadata-cluster", "--zookeeper", localZkConnection,
                "--configuration-store", localZkConnection, "--existing-bk-metadata-service-uri",
                "zk+null://" + bookieZkConnection + chroot, "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443", "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls", "pulsar+ssl://127.0.0.1:6651"};

        PulsarClusterMetadataSetup.main(initArgs);

        String[] teardownArgs = {"--cluster", "testSetupWithBkMetadata-cluster", "--zookeeper", localZkConnection,
                "--configuration-store", localZkConnection, "--existing-bk-metadata-service-uri",
                "zk+null://" + bookieZkConnection + chroot};

        ZooKeeper localZk = PulsarClusterMetadataTeardown.initZk(localZkConnection, 300000);

        // expected exist
        assertNotNull(localZk.exists("/managed-ledgers", false));

        // mock bookie shell format, create zk necessary node
        ZooKeeper bookieZk = PulsarClusterMetadataTeardown.initZk(bookieZkConnection, 300000);
        ZkUtils.createFullPathOptimistic(bookieZk, chroot + "/available", "{}".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        assertNotNull(bookieZk.exists(chroot, false));

        // teardown
        PulsarClusterMetadataTeardown.main(teardownArgs);

        // localZk expected not exist
        assertNull(localZk.exists("/managed-ledgers", false));

        // localZk "/zookeeper" only
        assertEquals(1, localZk.getChildren("/", false).size());

        // bookieZk expected exist
        assertNotNull(bookieZk.exists(chroot, false));

    }

    @BeforeMethod
    void setup() throws Exception {
        localZkS = new ZookeeperServerTest(0);
        localZkS.start();

        bookieZkS = new ZookeeperServerTest(0);
        bookieZkS.start();
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        localZkS.close();
        bookieZkS.close();
    }


}
