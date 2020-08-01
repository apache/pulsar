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

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.common.util.concurrent.AtomicDouble;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.PulsarClusterMetadataSetup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.zookeeper.aspectj.ClientCnxnAspect;
import org.apache.pulsar.broker.zookeeper.aspectj.ClientCnxnAspect.EventListner;
import org.apache.pulsar.broker.zookeeper.aspectj.ClientCnxnAspect.EventType;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType;
import org.apache.pulsar.zookeeper.ZookeeperBkClientFactoryImpl;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ZooKeeperClientAspectJTest {

    private ZookeeperServerTest localZkS;
    private ZooKeeper localZkc;
    private final long ZOOKEEPER_SESSION_TIMEOUT_MILLIS = 2000;
    private final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    static {
        // load agent with aspectjweaver-Agent for testing
        // maven-test waves advice on build-goal so, maven doesn't need explicit loading
        // uncomment it while testing on eclipse:
        // AgentLoader.loadAgentClass(Agent.class.getName(), null);
    }

    @Test
    public void testZkConnected() throws Exception {
        OrderedScheduler executor = OrderedScheduler.newSchedulerBuilder().build();
        try {
            ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
            CompletableFuture<ZooKeeper> zkFuture = zkf.create("127.0.0.1:" + localZkS.getZookeeperPort(),
                    SessionType.ReadWrite,
                    (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
            localZkc = zkFuture.get(ZOOKEEPER_SESSION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            assertTrue(localZkc.getState().isConnected());
            assertNotEquals(localZkc.getState(), States.CONNECTEDREADONLY);
        }finally{
            if (localZkc != null) {
                localZkc.close();
            }

            executor.shutdown();
        }
    }

    @Test
    public void testInitZk() throws Exception {
        try {
            ZooKeeperClientFactory zkfactory = new ZookeeperClientFactoryImpl();
            CompletableFuture<ZooKeeper> zkFuture = zkfactory.create("127.0.0.1:" + localZkS.getZookeeperPort(),
                    SessionType.ReadWrite,
                    (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
            localZkc = zkFuture.get(ZOOKEEPER_SESSION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            assertTrue(localZkc.getState().isConnected());
            assertNotEquals(localZkc.getState(), States.CONNECTEDREADONLY);

            String connection = "127.0.0.1:" + localZkS.getZookeeperPort() + "/prefix";
            ZooKeeper chrootZkc = PulsarClusterMetadataSetup.initZk(connection, (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
            assertTrue(chrootZkc.getState().isConnected());
            assertNotEquals(chrootZkc.getState(), States.CONNECTEDREADONLY);
            chrootZkc.close();

            assertNotNull(localZkc.exists("/prefix", false));
        } finally {
            if (localZkc != null) {
                localZkc.close();
            }
        }
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

    /**
     * Verifies that aspect-advice calculates the latency of of zk-operation
     *
     * @throws Exception
     */
    @Test(enabled=false, timeOut = 7000)
    void testZkClientAspectJTrigger() throws Exception {
        OrderedScheduler executor = OrderedScheduler.newSchedulerBuilder().build();
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("127.0.0.1:" + localZkS.getZookeeperPort(),
                SessionType.ReadWrite,
                (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
        localZkc = zkFuture.get(ZOOKEEPER_SESSION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        try {
            assertTrue(localZkc.getState().isConnected());
            assertNotEquals(localZkc.getState(), States.CONNECTEDREADONLY);

            final AtomicInteger writeCount = new AtomicInteger(0);
            final AtomicInteger readCount = new AtomicInteger(0);
            EventListner listener = new EventListner() {
                @Override
                public void recordLatency(EventType eventType, long latencyMiliSecond) {
                    if (eventType.equals(EventType.write)) {
                        writeCount.incrementAndGet();
                    } else if (eventType.equals(EventType.read)) {
                        readCount.incrementAndGet();
                    }
                }
            };
            ClientCnxnAspect.addListener(listener);
            CountDownLatch createLatch = new CountDownLatch(1);
            CountDownLatch deleteLatch = new CountDownLatch(1);
            CountDownLatch readLatch = new CountDownLatch(1);
            CountDownLatch existLatch = new CountDownLatch(1);
            localZkc.create("/createTest", "data".getBytes(), Acl, CreateMode.EPHEMERAL, (rc, path, ctx, name) -> {
                createLatch.countDown();
            }, "create");
            localZkc.delete("/deleteTest", -1, (rc, path, ctx) -> {
                deleteLatch.countDown();
            }, "delete");
            localZkc.exists("/createTest", null, (int rc, String path, Object ctx, Stat stat) -> {
                existLatch.countDown();
            }, null);
            localZkc.getData("/createTest", null, (int rc, String path, Object ctx, byte data[], Stat stat) -> {
                readLatch.countDown();
            }, null);
            createLatch.await();
            deleteLatch.await();
            existLatch.await();
            readLatch.await();
            Thread.sleep(500);
            Assert.assertEquals(writeCount.get(), 2);
            Assert.assertEquals(readCount.get(), 2);
            ClientCnxnAspect.removeListener(listener);
        } finally {
            if (localZkc != null) {
                localZkc.close();
            }

            executor.shutdown();
        }
    }

    /**
     * Verifies that aspect-advice calculates the latency of of zk-operation and updates PulsarStats
     *
     * @throws Exception
     */
    @Test(enabled=false, timeOut = 7000)
    public void testZkOpStatsMetrics() throws Exception {
        OrderedScheduler executor = OrderedScheduler.newSchedulerBuilder().build();
        ZooKeeperClientFactory zkf = new ZookeeperBkClientFactoryImpl(executor);
        CompletableFuture<ZooKeeper> zkFuture = zkf.create("127.0.0.1:" + localZkS.getZookeeperPort(),
                SessionType.ReadWrite,
                (int) ZOOKEEPER_SESSION_TIMEOUT_MILLIS);
        localZkc = zkFuture.get(ZOOKEEPER_SESSION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        MockPulsar mockPulsar = new MockPulsar(localZkc);
        mockPulsar.setup();
        try {
            PulsarClient pulsarClient = mockPulsar.getClient();
            PulsarService pulsar = mockPulsar.getPulsar();

            pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1").create();
            Metrics zkOpMetric = getMetric(pulsar, "zk_write_latency");
            Assert.assertNotNull(zkOpMetric);
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_write_rate_s"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_write_time_95percentile_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_write_time_99_99_percentile_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_write_time_99_9_percentile_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_write_time_99_percentile_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_write_time_mean_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_write_time_median_ms"));

            zkOpMetric = getMetric(pulsar, "zk_read_latency");
            Assert.assertNotNull(zkOpMetric);
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_read_rate_s"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_read_time_95percentile_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_read_time_99_99_percentile_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_read_time_99_9_percentile_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_read_time_99_percentile_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_read_time_mean_ms"));
            Assert.assertTrue(zkOpMetric.getMetrics().containsKey("brk_zk_read_time_median_ms"));

            CountDownLatch createLatch = new CountDownLatch(1);
            CountDownLatch deleteLatch = new CountDownLatch(1);
            CountDownLatch readLatch = new CountDownLatch(1);
            CountDownLatch existLatch = new CountDownLatch(1);
            localZkc.create("/createTest", "data".getBytes(), Acl, CreateMode.EPHEMERAL, (rc, path, ctx, name) -> {
                createLatch.countDown();
            }, "create");
            localZkc.delete("/deleteTest", -1, (rc, path, ctx) -> {
                deleteLatch.countDown();
            }, "delete");
            localZkc.exists("/createTest", null, (int rc, String path, Object ctx, Stat stat) -> {
                existLatch.countDown();
            }, null);
            localZkc.getData("/createTest", null, (int rc, String path, Object ctx, byte data[], Stat stat) -> {
                readLatch.countDown();
            }, null);
            createLatch.await();
            deleteLatch.await();
            existLatch.await();
            readLatch.await();
            Thread.sleep(10);

            BrokerService brokerService = pulsar.getBrokerService();
            brokerService.updateRates();
            List<Metrics> metrics = brokerService.getTopicMetrics();
            AtomicDouble writeRate = new AtomicDouble();
            AtomicDouble readRate = new AtomicDouble();
            metrics.forEach(m -> {
                if ("zk_write_latency".equalsIgnoreCase(m.getDimension("metric"))) {
                    writeRate.set((double) m.getMetrics().get("brk_zk_write_latency_rate_s"));
                } else if ("zk_read_latency".equalsIgnoreCase(m.getDimension("metric"))) {
                    readRate.set((double) m.getMetrics().get("brk_zk_read_latency_rate_s"));
                }
            });
            Assert.assertTrue(readRate.get() > 0);
            Assert.assertTrue(writeRate.get() > 0);
        } finally {
            mockPulsar.cleanup();
            if (localZkc != null) {
                localZkc.close();
            }

            executor.shutdown();
        }
    }

    private Metrics getMetric(PulsarService pulsar, String dimension) {
        BrokerService brokerService = pulsar.getBrokerService();
        brokerService.updateRates();
        for (Metrics metric : brokerService.getTopicMetrics()) {
            if (dimension.equalsIgnoreCase(metric.getDimension("metric"))) {
                return metric;
            }
        }
        return null;
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

        private final Logger log = LoggerFactory.getLogger(ZookeeperServerTest.class);
    }

    class MockPulsar extends BrokerTestBase {

        private final ZooKeeper zk;

        public MockPulsar(ZooKeeper zk) {
            this.zk = zk;
        }

        @Override
        protected void setup() throws Exception {
            super.baseSetup();
            doReturn(new ZooKeeperClientFactory() {
                @Override
                public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                        int zkSessionTimeoutMillis) {
                    return CompletableFuture.completedFuture(zk);
                }
            }).when(pulsar).getZooKeeperClientFactory();
        }

        @Override
        protected void cleanup() throws Exception {
            super.internalCleanup();
        }

        public PulsarService getPulsar() {
            return pulsar;
        }

        public PulsarClient getClient() {
            return pulsarClient;
        }

    }
}
