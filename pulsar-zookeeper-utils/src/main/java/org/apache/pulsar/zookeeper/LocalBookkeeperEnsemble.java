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
 * This file is derived from LocalBookkeeperEnsemble from Apache BookKeeper
 * http://bookkeeper.apache.org
 */

/*
 * Copyright 2011-2015 The Apache Software Foundation
 *
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
 *
 */
package org.apache.pulsar.zookeeper;

import static org.apache.commons.io.FileUtils.cleanDirectory;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.PrometheusMetricsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalBookkeeperEnsemble {
    protected static final Logger LOG = LoggerFactory.getLogger(LocalBookkeeperEnsemble.class);
    public static final int CONNECTION_TIMEOUT = 30000;

    int numberOfBookies;
    private boolean clearOldData = false;

    public LocalBookkeeperEnsemble(int numberOfBookies, int zkPort, int bkBasePort) {
        this(numberOfBookies, zkPort, bkBasePort, null, null, true);
    }

    public LocalBookkeeperEnsemble(int numberOfBookies, int zkPort, int bkBasePort, String zkDataDirName,
            String bkDataDirName, boolean clearOldData) {
        this.numberOfBookies = numberOfBookies;
        this.HOSTPORT = "127.0.0.1:" + zkPort;
        this.ZooKeeperDefaultPort = zkPort;
        this.initialPort = bkBasePort;
        this.zkDataDirName = zkDataDirName;
        this.bkDataDirName = bkDataDirName;
        this.clearOldData = clearOldData;
        LOG.info("Running " + this.numberOfBookies + " bookie(s).");
    }

    private final String HOSTPORT;
    NIOServerCnxnFactory serverFactory;
    ZooKeeperServer zks;
    ZooKeeper zkc;
    final int ZooKeeperDefaultPort;
    static int zkSessionTimeOut = 5000;
    String zkDataDirName;

    // BookKeeper variables
    String bkDataDirName;
    BookieServer bs[];
    ServerConfiguration bsConfs[];
    StatsProvider statsProviders[];
    Integer initialPort = 5000;

    /**
     * @param args
     */

    private void runZookeeper(int maxCC) throws IOException {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        LOG.info("Starting ZK server");
        // ServerStats.registerAsConcrete();
        // ClientBase.setupTestEnv();

        File zkDataDir = isNotBlank(zkDataDirName) ? Files.createDirectories(Paths.get(zkDataDirName)).toFile()
                : Files.createTempDirectory("zktest").toFile();

        if (this.clearOldData) {
            cleanDirectory(zkDataDir);
        }

        try {
            zks = new ZooKeeperServer(zkDataDir, zkDataDir, ZooKeeperServer.DEFAULT_TICK_TIME);
            serverFactory = new NIOServerCnxnFactory();
            serverFactory.configure(new InetSocketAddress(ZooKeeperDefaultPort), maxCC);
            serverFactory.startup(zks);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Exception while instantiating ZooKeeper", e);
        }

        boolean b = waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT);
        LOG.info("ZooKeeper server up: {}", b);
        LOG.debug("Local ZK started (port: {}, data_directory: {})", ZooKeeperDefaultPort, zkDataDir.getAbsolutePath());
    }

    private void initializeZookeper() throws IOException {
        LOG.info("Instantiate ZK Client");
        // initialize the zk client with values
        try {
            ZKConnectionWatcher zkConnectionWatcher = new ZKConnectionWatcher();
            zkc = new ZooKeeper(HOSTPORT, zkSessionTimeOut, zkConnectionWatcher);
            zkConnectionWatcher.waitForConnection();
            if (zkc.exists("/ledgers", false) == null) {
                zkc.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zkc.exists("/ledgers/available", false) == null) {
                zkc.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            // No need to create an entry for each requested bookie anymore as the
            // BookieServers will register themselves with ZooKeeper on startup.
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            LOG.error("Exception while creating znodes", e);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            LOG.error("Interrupted while creating znodes", e);
        }
    }

    private void runBookies(ServerConfiguration baseConf) throws Exception {
        LOG.info("Starting Bookie(s)");
        // Create Bookie Servers (B1, B2, B3)

        bs = new BookieServer[numberOfBookies];
        bsConfs = new ServerConfiguration[numberOfBookies];
        statsProviders = new StatsProvider[numberOfBookies];

        for (int i = 0; i < numberOfBookies; i++) {

            File bkDataDir = isNotBlank(bkDataDirName)
                    ? Files.createDirectories(Paths.get(bkDataDirName + Integer.toString(i))).toFile()
                    : Files.createTempDirectory("bk" + Integer.toString(i) + "test").toFile();

            if (this.clearOldData) {
                cleanDirectory(bkDataDir);
            }

            bsConfs[i] = new ServerConfiguration(baseConf);
            // override settings
            bsConfs[i].setBookiePort(initialPort + i);
            bsConfs[i].setZkServers("127.0.0.1:" + ZooKeeperDefaultPort);
            bsConfs[i].setJournalDirName(bkDataDir.getPath());
            bsConfs[i].setLedgerDirNames(new String[] { bkDataDir.getPath() });
            bsConfs[i].setAllowLoopback(true);
            bsConfs[i].setGcWaitTime(60000);

            // Initialize Stats Provider only if we're running with 1 single bookie
            // to avoid port conflicts
            if (numberOfBookies == 1) {
                statsProviders[i] = new PrometheusMetricsProvider();
                statsProviders[i].start(bsConfs[i]);
            }

            StatsLogger statsLogger = statsProviders[i].getStatsLogger("");
            bs[i] = new BookieServer(bsConfs[i], statsLogger);
            bs[i].start();
            LOG.debug("Local BK[{}] started (port: {}, data_directory: {})", i, initialPort + i,
                    bkDataDir.getAbsolutePath());
        }
    }

    public void start() throws Exception {
        LOG.debug("Local ZK/BK starting ...");
        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory");
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setProperty("dbStorage_rocksDBEnabled", true);
        conf.setProperty("dbStorage_writeCacheMaxSizeMb", 256);
        conf.setProperty("dbStorage_readAheadCacheMaxSizeMb", 64);
        conf.setFlushInterval(60000);
        conf.setProperty("journalMaxGroupWaitMSec", 1L);

        runZookeeper(1000);
        initializeZookeper();
        runBookies(conf);
    }

    public void stop() throws Exception {
        LOG.debug("Local ZK/BK stopping ...");
        for (BookieServer bookie : bs) {
            bookie.shutdown();
        }

        for (StatsProvider statsProvider : statsProviders) {
            statsProvider.stop();
        }

        zkc.close();
        zks.shutdown();
        serverFactory.shutdown();
        LOG.debug("Local ZK/BK stopped");
    }

    /* Watching SyncConnected event from ZooKeeper */
    public static class ZKConnectionWatcher implements Watcher {
        private CountDownLatch clientConnectLatch = new CountDownLatch(1);

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.SyncConnected) {
                clientConnectLatch.countDown();
            }
        }

        // Waiting for the SyncConnected event from the ZooKeeper server
        public void waitForConnection() throws IOException {
            try {
                if (!clientConnectLatch.await(zkSessionTimeOut, TimeUnit.MILLISECONDS)) {
                    throw new IOException("Couldn't connect to zookeeper server");
                }
            } catch (InterruptedException e) {
                throw new IOException("Interrupted when connecting to zookeeper server", e);
            }
        }
    }

    public static boolean waitForServerUp(String hp, long timeout) {
        long start = MathUtils.now();
        String split[] = hp.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        while (true) {
            try {
                Socket sock = new Socket(host, port);
                BufferedReader reader = null;
                try {
                    OutputStream outstream = sock.getOutputStream();
                    outstream.write("stat".getBytes());
                    outstream.flush();

                    reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    String line = reader.readLine();
                    if (line != null && line.startsWith("Zookeeper version:")) {
                        LOG.info("Server UP");
                        return true;
                    }
                } finally {
                    sock.close();
                    if (reader != null) {
                        reader.close();
                    }
                }
            } catch (IOException e) {
                // ignore as this is expected
                LOG.info("server " + hp + " not up " + e);
            }

            if (MathUtils.now() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

    public ZooKeeper getZkClient() {
        return zkc;
    }

    public ZooKeeperServer getZkServer() {
        return zks;
    }

    public BookieServer[] getBookies() {
        return bs;
    }
}
