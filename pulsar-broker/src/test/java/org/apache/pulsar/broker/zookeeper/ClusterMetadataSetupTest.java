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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClientBuilder;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.pulsar.PulsarClusterMetadataSetup;
import org.apache.pulsar.PulsarInitialNamespaceSetup;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ClusterMetadataSetupTest {
    private ZookeeperServerTest localZkS;

    // test SetupClusterMetadata several times, all should be successful
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
        SortedMap<String, String> data1 = localZkS.dumpData();
        PulsarClusterMetadataSetup.main(args);
        SortedMap<String, String> data2 = localZkS.dumpData();
        assertEquals(data1, data2);
        PulsarClusterMetadataSetup.main(args);
        SortedMap<String, String> data3 = localZkS.dumpData();
        assertEquals(data1, data3);
    }

    @DataProvider(name = "useMetadataStoreUrl")
    public static Object[][] useMetadataStoreUrlDataSet() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Test(dataProvider = "useMetadataStoreUrl")
    public void testSetupClusterWithMultiZkServers(boolean useMetadataStoreUrl) throws Exception {
        HashSet<String> firstLevelNodes = new HashSet<>(Arrays.asList(
                "bookies", "ledgers", "pulsar", "stream", "admin"
        ));
        final String metadataStoreParam;
        final String metadataStoreValue;
        if (useMetadataStoreUrl) {
            metadataStoreParam = "--metadata-store";
            metadataStoreValue = "zk:127.0.0.1:" + localZkS.getZookeeperPort()
                    + ",127.0.0.1:" + localZkS.getZookeeperPort() + "/test";
        } else {
            metadataStoreParam = "--zookeeper";
            metadataStoreValue = "127.0.0.1:" + localZkS.getZookeeperPort()
                    + ",127.0.0.1:" + localZkS.getZookeeperPort() + "/test";
        }
        String[] args = {
                "--cluster", "testReSetupClusterMetadata-cluster",
                metadataStoreParam, metadataStoreValue,
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort() + "/test",
                "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443",
                "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls","pulsar+ssl://127.0.0.1:6651"
        };
        PulsarClusterMetadataSetup.main(args);

        try (ZooKeeper zk = ZooKeeperClient.newBuilder()
                .connectString("127.0.0.1:" + localZkS.getZookeeperPort())
                .build()) {
            assertNotNull(zk.exists("/test", false));
            assertEquals(new HashSet<>(zk.getChildren("/test", false)), firstLevelNodes);
            assertEquals(new HashSet<>(zk.getChildren("/test/ledgers", false)),
                    new HashSet<>(Arrays.asList("available", "INSTANCEID")));

        }
    }

    @Test(dataProvider = "useMetadataStoreUrl")
    public void testSetupClusterDefault(boolean useMetadataStoreUrl) throws Exception {
        HashSet<String> firstLevelNodes = new HashSet<>(Arrays.asList(
                "bookies", "ledgers", "pulsar", "stream", "admin", "zookeeper"
        ));
        final String metadataStoreParam;
        final String metadataStoreValue;
        if (useMetadataStoreUrl) {
            metadataStoreParam = "--metadata-store";
            metadataStoreValue = "zk:127.0.0.1:" + localZkS.getZookeeperPort();
        } else {
            metadataStoreParam = "--zookeeper";
            metadataStoreValue = "127.0.0.1:" + localZkS.getZookeeperPort();
        }
        String[] args = {
                "--cluster", "testReSetupClusterMetadata-cluster",
                metadataStoreParam, metadataStoreValue,
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443",
                "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls","pulsar+ssl://127.0.0.1:6651"
        };
        PulsarClusterMetadataSetup.main(args);

        try (ZooKeeper zk = ZooKeeperClient.newBuilder()
                .connectString("127.0.0.1:" + localZkS.getZookeeperPort())
                .build()) {
            assertNotNull(zk.exists("/", false));
            assertEquals(new HashSet<>(zk.getChildren("/", false)), firstLevelNodes);
            assertEquals(new HashSet<>(zk.getChildren("/ledgers", false)),
                    new HashSet<>(Arrays.asList("available", "INSTANCEID")));

        }
    }

    @Test(dataProvider = "useMetadataStoreUrl")
    public void testSetupClusterInChrootMode(boolean useMetadataStoreUrl) throws Exception {
        HashSet<String> firstLevelNodes = new HashSet<>(Arrays.asList(
                "bookies", "ledgers", "pulsar", "stream", "admin"
        ));
        final String rootPath = "/test-prefix";
        final String metadataStoreParam;
        final String metadataStoreValue;
        if (useMetadataStoreUrl) {
            metadataStoreParam = "--metadata-store";
            metadataStoreValue = "zk:127.0.0.1:" + localZkS.getZookeeperPort() + rootPath;
        } else {
            metadataStoreParam = "--zookeeper";
            metadataStoreValue = "127.0.0.1:" + localZkS.getZookeeperPort() + rootPath;
        }

        String[] args = {
                "--cluster", "testReSetupClusterMetadata-cluster",
                metadataStoreParam, metadataStoreValue,
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort() + rootPath,
                "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443",
                "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls","pulsar+ssl://127.0.0.1:6651"
        };
        PulsarClusterMetadataSetup.main(args);

        try (ZooKeeper zk = ZooKeeperClient.newBuilder()
                .connectString("127.0.0.1:" + localZkS.getZookeeperPort())
                .build()) {
            assertNotNull(zk.exists(rootPath, false));
            assertEquals(new HashSet<>(zk.getChildren(rootPath, false)), firstLevelNodes);
        }
    }

    @Test(dataProvider = "useMetadataStoreUrl")
    public void testSetupClusterInNestedChrootMode(boolean useMetadataStoreUrl) throws Exception {
        HashSet<String> firstLevelNodes = new HashSet<>(Arrays.asList(
                "bookies", "ledgers", "pulsar", "stream", "admin"
        ));
        final String rootPath = "/test-prefix/nested";
        final String metadataStoreParam;
        final String metadataStoreValue;
        if (useMetadataStoreUrl) {
            metadataStoreParam = "--metadata-store";
            metadataStoreValue = "zk:127.0.0.1:" + localZkS.getZookeeperPort() + rootPath;
        } else {
            metadataStoreParam = "--zookeeper";
            metadataStoreValue = "127.0.0.1:" + localZkS.getZookeeperPort() + rootPath;
        }
        String[] args = {
                "--cluster", "testReSetupClusterMetadata-cluster",
                metadataStoreParam, metadataStoreValue,
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort() + rootPath,
                "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443",
                "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls","pulsar+ssl://127.0.0.1:6651"
        };
        PulsarClusterMetadataSetup.main(args);

        try (ZooKeeper zk = ZooKeeperClient.newBuilder()
                .connectString("127.0.0.1:" + localZkS.getZookeeperPort())
                .build()) {
            assertNotNull(zk.exists(rootPath, false));
            assertEquals(new HashSet<>(zk.getChildren(rootPath, false)), firstLevelNodes);
        }
    }

    @Test
    public void testSetupWithBkMetadataServiceUri() throws Exception {
        String zkConnection = "127.0.0.1:" + localZkS.getZookeeperPort();
        String[] args = {
                "--cluster", "testReSetupClusterMetadata-cluster",
                "--zookeeper", zkConnection,
                "--configuration-store", zkConnection,
                "--existing-bk-metadata-service-uri", "zk+null://" + zkConnection + "/chroot/ledgers",
                "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443",
                "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls","pulsar+ssl://127.0.0.1:6651"
        };

        PulsarClusterMetadataSetup.main(args);

        try (MetadataStoreExtended localStore = PulsarClusterMetadataSetup
                .initMetadataStore(zkConnection, 30000)) {
            // expected not exist
            assertFalse(localStore.exists("/ledgers").get());

            String[] bookkeeperMetadataServiceUriArgs = {
                    "--cluster", "testReSetupClusterMetadata-cluster",
                    "--zookeeper", zkConnection,
                    "--configuration-store", zkConnection,
                    "--bookkeeper-metadata-service-uri", "zk+null://" + zkConnection + "/chroot/ledgers",
                    "--web-service-url", "http://127.0.0.1:8080",
                    "--web-service-url-tls", "https://127.0.0.1:8443",
                    "--broker-service-url", "pulsar://127.0.0.1:6650",
                    "--broker-service-url-tls", "pulsar+ssl://127.0.0.1:6651"
            };

            PulsarClusterMetadataSetup.main(bookkeeperMetadataServiceUriArgs);
            try (MetadataStoreExtended bookkeeperMetadataServiceUriStore = PulsarClusterMetadataSetup
                    .initMetadataStore(zkConnection, 30000)) {
                // expected not exist
                assertFalse(bookkeeperMetadataServiceUriStore.exists("/ledgers").get());
            }

            String[] args1 = {
                    "--cluster", "testReSetupClusterMetadata-cluster",
                    "--zookeeper", zkConnection,
                    "--configuration-store", zkConnection,
                    "--web-service-url", "http://127.0.0.1:8080",
                    "--web-service-url-tls", "https://127.0.0.1:8443",
                    "--broker-service-url", "pulsar://127.0.0.1:6650",
                    "--broker-service-url-tls", "pulsar+ssl://127.0.0.1:6651"
            };

            PulsarClusterMetadataSetup.main(args1);

            // expected exist
            assertTrue(localStore.exists("/ledgers").get());
        }
    }

    @Test
    public void testInitialNamespaceSetup() throws Exception {
        // missing arguments
        assertEquals(PulsarInitialNamespaceSetup.doMain(new String[]{}), 1);
        // invalid namespace
        assertEquals(PulsarInitialNamespaceSetup.doMain(new String[]{
                "--cluster", "testInitialNamespaceSetup-cluster",
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "a/b/c/d"
        }), 1);

        String[] args = {
                "--cluster", "testInitialNamespaceSetup-cluster",
                "--configuration-store", "127.0.0.1:" + localZkS.getZookeeperPort(),
                "test/a",
                "test/b",
                "test/c",
        };
        assertEquals(PulsarInitialNamespaceSetup.doMain(args), 0);
        try (MetadataStoreExtended store = MetadataStoreExtended.create("127.0.0.1:" + localZkS.getZookeeperPort(),
                MetadataStoreConfig.builder().build())) {
            TenantResources tenantResources = new TenantResources(store,
                    PulsarResources.DEFAULT_OPERATION_TIMEOUT_SEC);
            List<String> namespaces = tenantResources.getListOfNamespaces("test");
            assertEquals(new HashSet<>(namespaces), new HashSet<>(Arrays.asList("test/a", "test/b", "test/c")));
        }
    }

    @Test
    public void testInitialNamespaceSetupZKDefaultsFallback() throws Exception {

        final String zkServers = "127.0.0.1:" + localZkS.getZookeeperPort();
        String[] args = {
                "--cluster", "testInitialNamespaceSetupZKDefaultsFallback-cluster",
                "--configuration-store", zkServers,
                "--zookeeper", zkServers,
                "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443",
                "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls", "pulsar+ssl://127.0.0.1:6651"
        };
        PulsarClusterMetadataSetup.main(args);
        log.info("zkdata:" + localZkS.dumpData());
        BKDLConfig dlConfig = new BKDLConfig(zkServers, "/ledgers");
        DLMetadata dlMetadata = DLMetadata.create(dlConfig);

        URI dlogUri = WorkerUtils.newDlogNamespaceURI(zkServers);

        try {
            dlMetadata.create(dlogUri);
            Assert.fail("DLog Metadata hasn't been initialized correctly");
        } catch (ZKException e) {
            if (e.getKeeperExceptionCode() == KeeperException.Code.NODEEXISTS) {
                log.info("OK. DLog Metadata has been initialized correctly");
            } else {
                throw e;
            }
        }

        @Cleanup
        final org.apache.distributedlog.ZooKeeperClient zkc = ZooKeeperClientBuilder
                .newBuilder()
                .zkServers(zkServers)
                .sessionTimeoutMs(20000)
                .zkAclId(null)
                .build();
        BKDLConfig bkdlConfigFromZk = BKDLConfig.resolveDLConfig(zkc, dlogUri);
        assertEquals(bkdlConfigFromZk.getBkLedgersPath(), "/ledgers");

    }


    @Test
    public void testInitialNamespaceSetupZKDefaultsFallbackWithChroot() throws Exception {

        final String zkServers = "127.0.0.1:" + localZkS.getZookeeperPort();
        final String chrootPath = "/my-chroot";
        String[] args = {
                "--cluster", "testInitialNamespaceSetupZKDefaultsFallback-cluster",
                "--configuration-store", zkServers + chrootPath,
                "--zookeeper", zkServers + chrootPath,
                "--web-service-url", "http://127.0.0.1:8080",
                "--web-service-url-tls", "https://127.0.0.1:8443",
                "--broker-service-url", "pulsar://127.0.0.1:6650",
                "--broker-service-url-tls", "pulsar+ssl://127.0.0.1:6651"
        };
        PulsarClusterMetadataSetup.main(args);
        BKDLConfig dlConfig = new BKDLConfig(zkServers, chrootPath + "/ledgers");
        DLMetadata dlMetadata = DLMetadata.create(dlConfig);

        URI dlogUri = WorkerUtils.newDlogNamespaceURI(zkServers + chrootPath);

        try {
            dlMetadata.create(dlogUri);
            Assert.fail("DLog Metadata hasn't been initialized correctly");
        } catch (ZKException e) {
            if (e.getKeeperExceptionCode() == KeeperException.Code.NODEEXISTS) {
                log.info("OK. DLog Metadata has been initialized correctly");
            } else {
                throw e;
            }
        }

        @Cleanup
        final org.apache.distributedlog.ZooKeeperClient zkc = ZooKeeperClientBuilder
                .newBuilder()
                .zkServers(zkServers)
                .sessionTimeoutMs(20000)
                .zkAclId(null)
                .build();
        BKDLConfig bkdlConfigFromZk = BKDLConfig.resolveDLConfig(zkc, dlogUri);
        assertEquals(bkdlConfigFromZk.getBkLedgersPath(), chrootPath + "/ledgers");

    }

    @BeforeMethod
    void setup() throws Exception {
        localZkS = new ZookeeperServerTest(0);
        localZkS.start();
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        localZkS.close();
    }

    static class ZookeeperServerTest implements Closeable {
        private final File zkTmpDir;
        private ZooKeeperServer zks;
        private NIOServerCnxnFactory serverFactory;
        private final int zkPort;
        private final String hostPort;

        public ZookeeperServerTest(int zkPort) throws IOException {
            this.zkPort = zkPort;
            this.hostPort = "127.0.0.1:" + zkPort;
            this.zkTmpDir = File.createTempFile("zookeeper", "test");
            log.info("**** Start GZK on {} ****", zkTmpDir);
            if (!zkTmpDir.delete() || !zkTmpDir.mkdir()) {
                throw new IOException("Couldn't create zk directory " + zkTmpDir);
            }
        }

        public void start() throws IOException {
            try {
                zks = new ZooKeeperServer(zkTmpDir, zkTmpDir, ZooKeeperServer.DEFAULT_TICK_TIME);
                zks.setMaxSessionTimeout(20000);
                serverFactory = new NIOServerCnxnFactory();
                serverFactory.configure(new InetSocketAddress(zkPort), 1000);
                serverFactory.startup(zks);
            } catch (Exception e) {
                log.error("Exception while instantiating ZooKeeper", e);
            }

            LocalBookkeeperEnsemble.waitForServerUp(hostPort, 30000);
            log.info("ZooKeeper started at {}", hostPort);
        }

        public void stop() throws IOException {
            zks.shutdown();
            serverFactory.shutdown();
            log.info("Stoppend ZK server at {}", hostPort);
        }

        @Override
        public void close() throws IOException {
            zks.shutdown();
            serverFactory.shutdown();
            zkTmpDir.delete();
        }

        public int getZookeeperPort() {
            return serverFactory.getLocalPort();
        }

        public SortedMap<String, String> dumpData() throws IOException, InterruptedException, KeeperException {
            SortedMap<String, String> data = new TreeMap<>();
            try (ZooKeeper zk = ZooKeeperClient.newBuilder()
                    .connectString("127.0.0.1:" + getZookeeperPort())
                    .sessionTimeoutMs(20000)
                    .build()) {
                for (String child : zk.getChildren("/", false)) {
                    if ("zookeeper".equals(child)) {
                        continue;
                    }
                    dumpPath(zk, "/" + child, data);
                }
            }
            return data;
        }

        private void dumpPath(ZooKeeper zk, String path, SortedMap<String, String> dataMap)
                throws InterruptedException, KeeperException {
            dataMap.put(path, new String(zk.getData(path, false, null), Charset.defaultCharset()));
            for (String child : zk.getChildren(path, false)) {
                dumpPath(zk, path + "/" + child, dataMap);
            }
        }
    }


}
