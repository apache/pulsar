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

package org.apache.pulsar.zookeeper;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.apache.commons.io.FileUtils.cleanDirectory;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.bookkeeper.bookie.BookieException.InvalidCookieException;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.NamespaceExistsException;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.Backoff.Jitter.Type;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.server.StreamStorageLifecycleComponent;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterInitializer;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterInitializer;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalBookkeeperEnsemble {
    protected static final Logger LOG = LoggerFactory.getLogger(LocalBookkeeperEnsemble.class);
    public static final int CONNECTION_TIMEOUT = 30000;

    int numberOfBookies;
    private final boolean clearOldData;

    private static class BasePortManager implements Supplier<Integer> {

        private int port;

        public BasePortManager(int basePort) {
            this.port = basePort;
        }

        @Override
        public synchronized Integer get() {
            return port++;
        }
    }

    private final Supplier<Integer> portManager;


    public LocalBookkeeperEnsemble(int numberOfBookies, int zkPort, Supplier<Integer> portManager) {
        this(numberOfBookies, zkPort, 4181, null, null, true, null, portManager);
    }

    public LocalBookkeeperEnsemble(int numberOfBookies, int zkPort, int bkBasePort, String zkDataDirName,
            String bkDataDirName, boolean clearOldData) {
        this(numberOfBookies, zkPort, bkBasePort, 4181, zkDataDirName, bkDataDirName, clearOldData, null);
    }

    public LocalBookkeeperEnsemble(int numberOfBookies, int zkPort, int bkBasePort, String zkDataDirName,
            String bkDataDirName, boolean clearOldData, String advertisedAddress) {
        this(numberOfBookies, zkPort, bkBasePort, 4181, zkDataDirName, bkDataDirName, clearOldData, advertisedAddress);
    }

    public LocalBookkeeperEnsemble(int numberOfBookies,
                                   int zkPort,
                                   int bkBasePort,
                                   int streamStoragePort,
                                   String zkDataDirName,
                                   String bkDataDirName,
                                   boolean clearOldData,
                                   String advertisedAddress) {
        this(numberOfBookies, zkPort, streamStoragePort, zkDataDirName, bkDataDirName, clearOldData, advertisedAddress,
                new BasePortManager(bkBasePort));
    }

    public LocalBookkeeperEnsemble(int numberOfBookies,
            int zkPort,
            int streamStoragePort,
            String zkDataDirName,
            String bkDataDirName,
            boolean clearOldData,
            String advertisedAddress,
            Supplier<Integer> portManager) {
        this.numberOfBookies = numberOfBookies;
        this.portManager = portManager;
        this.streamStoragePort = streamStoragePort;
        this.zkDataDirName = zkDataDirName;
        this.bkDataDirName = bkDataDirName;
        this.clearOldData = clearOldData;
        this.zkPort = zkPort;
        this.advertisedAddress = null == advertisedAddress ? "127.0.0.1" : advertisedAddress;
        LOG.info("Running {} bookie(s) and advertised them at {}.", this.numberOfBookies, this.advertisedAddress);
    }

    private String HOSTPORT;
    private final String advertisedAddress;
    private int zkPort;

    NIOServerCnxnFactory serverFactory;
    ZooKeeperServer zks;
    DatadirCleanupManager zkDataCleanupManager;
    ZooKeeper zkc;

    static int zkSessionTimeOut = 5000;
    String zkDataDirName;

    // BookKeeper variables
    String bkDataDirName;
    BookieServer bs[];
    ServerConfiguration bsConfs[];

    // Stream/Table Storage
    StreamStorageLifecycleComponent streamStorage;
    Integer streamStoragePort = 4181;

    // directories created by this instance
    // it is safe to drop them on stop
    List<File> temporaryDirectories = new ArrayList<>();

    private File createTempDirectory(String seed) throws IOException {
        File res = Files.createTempDirectory(seed).toFile();
        temporaryDirectories.add(res);
        return res;
    }

    private void runZookeeper(int maxCC) throws IOException {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        LOG.info("Starting ZK server");
        // ServerStats.registerAsConcrete();
        // ClientBase.setupTestEnv();

        File zkDataDir = isNotBlank(zkDataDirName) ? Files.createDirectories(Paths.get(zkDataDirName)).toFile()
                : createTempDirectory("zktest");

        if (this.clearOldData) {
            cleanDirectory(zkDataDir);
        }

        try {
            // Allow all commands on ZK control port
            System.setProperty("zookeeper.4lw.commands.whitelist", "*");
            zks = new ZooKeeperServer(zkDataDir, zkDataDir, ZooKeeperServer.DEFAULT_TICK_TIME);

            serverFactory = new NIOServerCnxnFactory();
            serverFactory.configure(new InetSocketAddress(zkPort), maxCC);
            serverFactory.startup(zks);

            zkDataCleanupManager = new DatadirCleanupManager(zkDataDir, zkDataDir, 3, 1 /* hour */);
            zkDataCleanupManager.start();
        } catch (Exception e) {
            LOG.error("Exception while instantiating ZooKeeper", e);

            if (serverFactory != null) {
                serverFactory.shutdown();
            }
            throw new IOException(e);
        }

        this.zkPort = serverFactory.getLocalPort();
        this.HOSTPORT = "127.0.0.1:" + zkPort;

        boolean b = waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT);

        LOG.info("ZooKeeper server up: {}", b);
        LOG.debug("Local ZK started (port: {}, data_directory: {})", zkPort, zkDataDir.getAbsolutePath());
    }

    public void disconnectZookeeper(ZooKeeper zooKeeper) {
        ServerCnxn serverCnxn = getZookeeperServerConnection(zooKeeper);
        try {
            LOG.info("disconnect ZK server side connection {}", serverCnxn);
            Class disconnectReasonClass = Class.forName("org.apache.zookeeper.server.ServerCnxn$DisconnectReason");
            Method method = serverCnxn.getClass().getMethod("close", disconnectReasonClass);
            method.invoke(serverCnxn, Stream.of(disconnectReasonClass.getEnumConstants()).filter(s->s.toString().equals("CONNECTION_CLOSE_FORCED")).findFirst().get());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ServerCnxn getZookeeperServerConnection(ZooKeeper zooKeeper) {
        return StreamSupport.stream(serverFactory.getConnections().spliterator(), false)
            .filter((cnxn) -> cnxn.getSessionId() == zooKeeper.getSessionId())
            .findFirst()
            .orElse(null);
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
            if (zkc.exists("/ledgers/available/readonly", false) == null) {
                zkc.create("/ledgers/available/readonly", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zkc.exists(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false) == null) {
                zkc.create(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, "{}".getBytes(), Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }

            // No need to create an entry for each requested bookie anymore as the
            // BookieServers will register themselves with ZooKeeper on startup.
        } catch (KeeperException e) {
            LOG.error("Exception while creating znodes", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while creating znodes", e);
        }
    }

    private void runBookies(ServerConfiguration baseConf) throws Exception {
        LOG.info("Starting Bookie(s)");
        // Create Bookie Servers (B1, B2, B3)

        bs = new BookieServer[numberOfBookies];
        bsConfs = new ServerConfiguration[numberOfBookies];

        for (int i = 0; i < numberOfBookies; i++) {

            File bkDataDir = isNotBlank(bkDataDirName)
                    ? Files.createDirectories(Paths.get(bkDataDirName + i)).toFile()
                    : createTempDirectory("bk" + Integer.toString(i) + "test");

            if (this.clearOldData) {
                cleanDirectory(bkDataDir);
            }

            int bookiePort = portManager.get();

            // Ensure registration Z-nodes are cleared when standalone service is restarted ungracefully
            String registrationZnode = String.format("/ledgers/available/%s:%d", baseConf.getAdvertisedAddress(), bookiePort);
            if (zkc.exists(registrationZnode, null) != null) {
                try {
                    zkc.delete(registrationZnode, -1);
                } catch (NoNodeException nne) {
                    // Ignore if z-node was just expired
                }
            }

            bsConfs[i] = new ServerConfiguration(baseConf);
            // override settings
            bsConfs[i].setBookiePort(bookiePort);
            bsConfs[i].setZkServers("127.0.0.1:" + zkPort);
            bsConfs[i].setJournalDirName(bkDataDir.getPath());
            bsConfs[i].setLedgerDirNames(new String[] { bkDataDir.getPath() });
            bsConfs[i].setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
            bsConfs[i].setAllowEphemeralPorts(true);

            try {
                bs[i] = new BookieServer(bsConfs[i], NullStatsLogger.INSTANCE, null);
            } catch (InvalidCookieException e) {
                // InvalidCookieException can happen if the machine IP has changed
                // Since we are running here a local bookie that is always accessed
                // from localhost, we can ignore the error
                for (String path : zkc.getChildren("/ledgers/cookies", false)) {
                    zkc.delete("/ledgers/cookies/" + path, -1);
                }

                // Also clean the on-disk cookie
                new File(new File(bkDataDir, "current"), "VERSION").delete();

                // Retry to start the bookie after cleaning the old left cookie
                bs[i] = new BookieServer(bsConfs[i], NullStatsLogger.INSTANCE, null);
            }
            bs[i].start();
            LOG.debug("Local BK[{}] started (port: {}, data_directory: {})", i, bookiePort,
                    bkDataDir.getAbsolutePath());
        }
    }

    public void runStreamStorage(CompositeConfiguration conf) throws Exception {
        String zkServers = "127.0.0.1:" + zkPort;
        String metadataServiceUriStr = "zk://" + zkServers + "/ledgers";
        URI metadataServiceUri = URI.create(metadataServiceUriStr);

        // zookeeper servers
        conf.setProperty("metadataServiceUri", metadataServiceUriStr);
        // dlog settings
        conf.setProperty("dlog.bkcEnsembleSize", 1);
        conf.setProperty("dlog.bkcWriteQuorumSize", 1);
        conf.setProperty("dlog.bkcAckQuorumSize", 1);
        // stream storage port
        conf.setProperty("storageserver.grpc.port", streamStoragePort);

        // storage server settings
        conf.setProperty("storage.range.store.dirs", bkDataDirName + "/ranges/data");

        // initialize the stream storage metadata
        ClusterInitializer initializer = new ZkClusterInitializer(zkServers);
        initializer.initializeCluster(metadataServiceUri, 2);

        // load the stream storage component
        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.loadConf(conf);
        BookieConfiguration bkConf = new BookieConfiguration(serverConf);

        this.streamStorage = new StreamStorageLifecycleComponent(bkConf, NullStatsLogger.INSTANCE);
        this.streamStorage.start();
        LOG.debug("Local BK stream storage started (port: {})", streamStoragePort);

        // create a default namespace
        try (StorageAdminClient admin = StorageClientBuilder.newBuilder()
             .withSettings(StorageClientSettings.newBuilder()
                 .serviceUri("bk://localhost:4181")
                 .backoffPolicy(Backoff.Jitter.of(
                     Type.EXPONENTIAL,
                     1000,
                     10000,
                     30
                 ))
                 .build())
            .buildAdmin()) {

            try {
                NamespaceProperties ns = FutureUtils.result(admin.getNamespace("default"));
                LOG.info("'default' namespace for table service : {}", ns);
            } catch (NamespaceNotFoundException nnfe) {
                LOG.info("Creating default namespace");
                try {
                    NamespaceProperties ns =
                        FutureUtils.result(admin.createNamespace("default", NamespaceConfiguration.newBuilder()
                            .setDefaultStreamConf(DEFAULT_STREAM_CONF)
                            .build()));
                    LOG.info("Successfully created 'default' namespace :\n{}", ns);
                } catch (NamespaceExistsException nee) {
                    // namespace already exists
                    LOG.warn("Namespace 'default' already existed.");
                }
            }
        }
    }

    public void start(boolean enableStreamStorage) throws  Exception {
        LOG.debug("Local ZK/BK starting ...");
        ServerConfiguration conf = new ServerConfiguration();
        // Use minimal configuration requiring less memory for unit tests
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setProperty("dbStorage_writeCacheMaxSizeMb", 2);
        conf.setProperty("dbStorage_readAheadCacheMaxSizeMb", 1);
        conf.setProperty("dbStorage_rocksDB_writeBufferSizeMB", 1);
        conf.setProperty("dbStorage_rocksDB_blockCacheSize", 1024 * 1024);
        conf.setFlushInterval(60000);
        conf.setJournalSyncData(false);
        conf.setProperty("journalMaxGroupWaitMSec", 0L);
        conf.setAllowLoopback(true);
        conf.setGcWaitTime(60000);
        conf.setNumAddWorkerThreads(0);
        conf.setNumReadWorkerThreads(0);
        conf.setNumHighPriorityWorkerThreads(0);
        conf.setNumJournalCallbackThreads(0);
        conf.setServerNumIOThreads(1);
        conf.setNumLongPollWorkerThreads(1);
        conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);

        runZookeeper(1000);
        initializeZookeper();
        runBookies(conf);

        if (enableStreamStorage) {
            runStreamStorage(new CompositeConfiguration());
        }
    }

    public void start() throws Exception {
        start(false);
    }

    public void startStandalone() throws Exception {
        startStandalone(new ServerConfiguration(), false);
    }

    public void startStandalone(ServerConfiguration conf, boolean enableStreamStorage) throws Exception {
        LOG.debug("Local ZK/BK starting ...");
        conf.setAdvertisedAddress(advertisedAddress);

        runZookeeper(1000);
        initializeZookeper();
        runBookies(conf);
        if (enableStreamStorage) {
            runStreamStorage(new CompositeConfiguration());
        }
    }

    public void stopBK(int i) {
        bs[i].shutdown();
    }

    public void stopBK() {
        LOG.debug("Local ZK/BK stopping ...");
        for (BookieServer bookie : bs) {
            bookie.shutdown();
        }
    }

    public void startBK(int i) throws Exception {
        try {
            bs[i] = new BookieServer(bsConfs[i], NullStatsLogger.INSTANCE, null);
        } catch (InvalidCookieException e) {
            // InvalidCookieException can happen if the machine IP has changed
            // Since we are running here a local bookie that is always accessed
            // from localhost, we can ignore the error
            for (String path : zkc.getChildren("/ledgers/cookies", false)) {
                zkc.delete("/ledgers/cookies/" + path, -1);
            }

            // Also clean the on-disk cookie
            new File(new File(bsConfs[i].getJournalDirNames()[0], "current"), "VERSION").delete();

            // Retry to start the bookie after cleaning the old left cookie
            bs[i] = new BookieServer(bsConfs[i], NullStatsLogger.INSTANCE, null);

        }
        bs[i].start();
    }

    public void startBK() throws Exception {
        for (int i = 0; i < numberOfBookies; i++) {
            startBK(i);
        }
    }

    public void stop() throws Exception {
        if (null != streamStorage) {
            LOG.debug("Local bk stream storage stopping ...");
            streamStorage.close();
        }

        LOG.debug("Local ZK/BK stopping ...");
        for (BookieServer bookie : bs) {
            try {
                bookie.shutdown();
            } catch (Exception e) {
                LOG.warn("failed to shutdown bookie", e);
            }
        }

        zkc.close();
        zks.shutdown();
        serverFactory.shutdown();

        if (zkDataCleanupManager != null) {
            zkDataCleanupManager.shutdown();
        }
        LOG.debug("Local ZK/BK stopped");
        for (File managedDir : temporaryDirectories) {
            LOG.info("deleting test directory {}", managedDir);
            FileUtils.deleteDirectory(managedDir);
        }
        temporaryDirectories.clear();
    }

    /* Watching SyncConnected event from ZooKeeper */
    public static class ZKConnectionWatcher implements Watcher {
        private final CountDownLatch clientConnectLatch = new CountDownLatch(1);

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
        long start = System.currentTimeMillis();
        String[] split = hp.split(":");
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

            if (System.currentTimeMillis() > start + timeout) {
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

    public int getZookeeperPort() {
        return zkPort;
    }
}
