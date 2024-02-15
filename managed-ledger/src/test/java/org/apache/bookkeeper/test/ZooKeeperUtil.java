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
/**
 * This file is derived from ZooKeeperUtil from Apache BookKeeper
 * http://bookkeeper.apache.org
 */

package org.apache.bookkeeper.test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public class ZooKeeperUtil {
    static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtil.class);

    // ZooKeeper related variables
    protected int zooKeeperPort;
    private InetSocketAddress zkaddr;

    protected ZooKeeperServer zks;
    protected ZooKeeper zkc; // zookeeper client
    protected NIOServerCnxnFactory serverFactory;
    protected File ZkTmpDir;
    private String connectString;

    public ZooKeeperUtil() {
        String loopbackIPAddr = InetAddress.getLoopbackAddress().getHostAddress();
        zkaddr = new InetSocketAddress(loopbackIPAddr, 0);
        connectString = loopbackIPAddr + ":" + zooKeeperPort;
    }

    public ZooKeeper getZooKeeperClient() {
        return zkc;
    }

    public String getZooKeeperConnectString() {
        return connectString;
    }

    public void startServer() throws Exception {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        LOG.debug("Running ZK server");
        // ServerStats.registerAsConcrete();
        ClientBase.setupTestEnv();
        ZkTmpDir = File.createTempFile("zookeeper", "test");
        ZkTmpDir.delete();
        ZkTmpDir.mkdir();

        zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperServer.DEFAULT_TICK_TIME);
        serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(zkaddr, 100);
        serverFactory.startup(zks);

        zooKeeperPort = serverFactory.getLocalPort();
        zkaddr = new InetSocketAddress(zooKeeperPort);
        connectString = "localhost:" + zooKeeperPort;

        boolean b = ClientBase.waitForServerUp(getZooKeeperConnectString(), ClientBase.CONNECTION_TIMEOUT);
        LOG.debug("Server up: " + b);

        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        zkc = ZooKeeperClient.newBuilder().connectString(getZooKeeperConnectString()).build();

        // initialize the zk client with values
        zkc.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void startServer(String path) throws Exception {
        LOG.debug("Running ZK server");
        // ServerStats.registerAsConcrete();
        ClientBase.setupTestEnv();
        ZkTmpDir = File.createTempFile("zookeeper", "test");
        ZkTmpDir.delete();
        ZkTmpDir.mkdir();

        zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperServer.DEFAULT_TICK_TIME);
        serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(zkaddr, 100);
        serverFactory.startup(zks);

        zooKeeperPort = serverFactory.getLocalPort();
        connectString = "localhost:" + zooKeeperPort;
        boolean b = ClientBase.waitForServerUp(getZooKeeperConnectString(), ClientBase.CONNECTION_TIMEOUT);
        LOG.debug("Server up: " + b);

        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        zkc = ZooKeeperClient.newBuilder().connectString(getZooKeeperConnectString()).build();
        if (!"".equals(path)) {
            zkc.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // initialize the zk client with values
        zkc.create(path + "/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create(path +"/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @SuppressWarnings("deprecation")
    public void sleepServer(final int seconds, final CountDownLatch l) throws InterruptedException, IOException {
        Thread[] allthreads = new Thread[Thread.activeCount()];
        Thread.enumerate(allthreads);
        for (final Thread t : allthreads) {
            if (t.getName().contains("SyncThread:0")) {
                Thread sleeper = new Thread() {
                    public void run() {
                        try {
                            t.suspend();
                            l.countDown();
                            Thread.sleep(seconds * 1000);
                            t.resume();
                        } catch (Exception e) {
                            LOG.error("Error suspending thread", e);
                        }
                    }
                };
                sleeper.start();
                return;
            }
        }
        throw new IOException("ZooKeeper thread not found");
    }

    public void killServer() throws Exception {
        if (zkc != null) {
            zkc.close();
        }

        // shutdown ZK server
        if (serverFactory != null) {
            serverFactory.shutdown();
            Assert.assertTrue(ClientBase.waitForServerDown(getZooKeeperConnectString(), ClientBase.CONNECTION_TIMEOUT),
                    "waiting for server down");
        }
        if (zks != null) {
            zks.getTxnLogFactory().close();
        }
        // ServerStats.unregister();
        FileUtils.deleteDirectory(ZkTmpDir);
    }
}
