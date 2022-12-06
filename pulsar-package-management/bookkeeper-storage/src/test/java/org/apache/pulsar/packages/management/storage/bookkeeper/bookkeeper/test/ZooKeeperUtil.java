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
package org.apache.pulsar.packages.management.storage.bookkeeper.bookkeeper.test;

import static org.testng.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperUtil implements ZooKeeperCluster {
    static final Logger LOG;
    protected Integer zooKeeperPort = 0;
    private InetSocketAddress zkaddr;
    protected ZooKeeperServer zks;
    protected ZooKeeper zkc;
    protected NIOServerCnxnFactory serverFactory;
    protected File zkTmpDir;
    private String connectString;

    public ZooKeeperUtil() {
        String loopbackIPAddr = InetAddress.getLoopbackAddress().getHostAddress();
        this.zkaddr = new InetSocketAddress(loopbackIPAddr, 0);
        this.connectString = loopbackIPAddr + ":" + this.zooKeeperPort;
    }

    public ZooKeeper getZooKeeperClient() {
        return this.zkc;
    }

    public String getZooKeeperConnectString() {
        return this.connectString;
    }

    public String getMetadataServiceUri() {
        return this.getMetadataServiceUri("/ledgers");
    }

    public String getMetadataServiceUri(String zkLedgersRootPath) {
        return "zk://" + this.connectString + zkLedgersRootPath;
    }

    public String getMetadataServiceUri(String zkLedgersRootPath, String type) {
        return "zk+" + type + "://" + this.connectString + zkLedgersRootPath;
    }

    public void startCluster() throws Exception {
        LOG.debug("Running ZK server");
        ClientBase.setupTestEnv();
        this.zkTmpDir = IOUtils.createTempDir("zookeeper", "test");
        this.restartCluster();
        this.createBKEnsemble("/ledgers");
    }

    public void restartCluster() throws Exception {
        this.zks = new ZooKeeperServer(this.zkTmpDir, this.zkTmpDir, 3000);
        this.serverFactory = new NIOServerCnxnFactory();
        this.serverFactory.configure(this.zkaddr, 100);
        this.serverFactory.startup(this.zks);
        if (0 == this.zooKeeperPort) {
            this.zooKeeperPort = this.serverFactory.getLocalPort();
            this.zkaddr = new InetSocketAddress(this.zkaddr.getAddress().getHostAddress(), this.zooKeeperPort);
            this.connectString = this.zkaddr.getAddress().getHostAddress() + ":" + this.zooKeeperPort;
        }

        boolean b = ClientBase.waitForServerUp(this.getZooKeeperConnectString(), (long)ClientBase.CONNECTION_TIMEOUT);
        LOG.debug("Server up: " + b);
        LOG.debug("Instantiate ZK Client");
        this.zkc = ZooKeeperClient.newBuilder().connectString(this.getZooKeeperConnectString()).sessionTimeoutMs(10000).build();
    }

    public void sleepCluster(final int time, final TimeUnit timeUnit, final CountDownLatch l) throws InterruptedException, IOException {
        Thread[] allthreads = new Thread[Thread.activeCount()];
        Thread.enumerate(allthreads);
        Thread[] var5 = allthreads;
        int var6 = allthreads.length;

        for(int var7 = 0; var7 < var6; ++var7) {
            final Thread t = var5[var7];
            if (t.getName().contains("SyncThread:0")) {
                Thread sleeper = new Thread() {
                    public void run() {
                        try {
                            t.suspend();
                            l.countDown();
                            timeUnit.sleep((long)time);
                            t.resume();
                        } catch (Exception var2) {
                            ZooKeeperUtil.LOG.error("Error suspending thread", var2);
                        }

                    }
                };
                sleeper.start();
                return;
            }
        }

        throw new IOException("ZooKeeper thread not found");
    }

    public void stopCluster() throws Exception {
        if (this.zkc != null) {
            this.zkc.close();
        }

        if (this.serverFactory != null) {
            this.serverFactory.shutdown();
            assertTrue(ClientBase.waitForServerDown(this.getZooKeeperConnectString(), (long)ClientBase.CONNECTION_TIMEOUT),"waiting for server down");
        }

        if (this.zks != null) {
            this.zks.getTxnLogFactory().close();
        }

    }

    public void killCluster() throws Exception {
        this.stopCluster();
        FileUtils.deleteDirectory(this.zkTmpDir);
    }

    static {
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
        LOG = LoggerFactory.getLogger(ZooKeeperUtil.class);
    }
}
