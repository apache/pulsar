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
package org.apache.pulsar.metadata.bookkeeper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.awaitility.Awaitility;
import org.testng.Assert;

/**
 * A class runs several bookie servers for testing.
 */
@Slf4j
class BKTestCluster implements AutoCloseable {

    // Metadata service related variables
    private final String metadataServiceUri;
    private final MetadataStoreExtended store;

    // BookKeeper related variables
    private final List<File> tmpDirs = new ArrayList<>();
    private final List<BookieServer> bs = new ArrayList<>();
    private final List<ServerConfiguration> bsConfs = new ArrayList<>();

    protected final ServerConfiguration baseConf = TestBKConfiguration.newServerConfiguration();
    protected final ClientConfiguration baseClientConf = TestBKConfiguration.newClientConfiguration();


    public BKTestCluster(String metadataServiceUri, int numBookies) throws Exception {
        this.metadataServiceUri = metadataServiceUri;
        this.store = MetadataStoreExtended.create(metadataServiceUri, MetadataStoreConfig.builder().build());
        baseConf.setJournalRemovePagesFromCache(false);
        baseConf.setProperty(AbstractMetadataDriver.METADATA_STORE_INSTANCE, store);
        baseClientConf.setProperty(AbstractMetadataDriver.METADATA_STORE_INSTANCE, store);
        System.setProperty("bookkeeper.metadata.bookie.drivers", PulsarMetadataBookieDriver.class.getName());
        System.setProperty("bookkeeper.metadata.client.drivers", PulsarMetadataClientDriver.class.getName());
        startBKCluster(numBookies);
    }

    private final Map<BookieServer, AutoRecoveryMain> autoRecoveryProcesses = new HashMap<>();

    @Getter
    boolean isAutoRecoveryEnabled = false;


    @Override
    public void close() throws Exception {
        boolean failed = false;
        // stop bookkeeper service
        try {
            stopBKCluster();
        } catch (Exception e) {
            log.error("Got Exception while trying to stop BKCluster", e);
        }
        // cleanup temp dirs
        try {
            cleanupTempDirs();
        } catch (Exception e) {
            log.error("Got Exception while trying to cleanupTempDirs", e);
        }

        this.store.close();
    }

    private File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tmpDirs.add(dir);
        return dir;
    }

    /**
     * Start cluster. Also, starts the auto recovery process for each bookie, if
     * isAutoRecoveryEnabled is true.
     *
     * @throws Exception
     */
    private void startBKCluster(int numBookies) throws Exception {
        PulsarRegistrationManager rm = new PulsarRegistrationManager(store, "/ledgers", baseConf);
        rm.initNewCluster();

        baseConf.setMetadataServiceUri("metadata-store:" + metadataServiceUri);
        baseClientConf.setMetadataServiceUri("metadata-store:" + metadataServiceUri);

        // Create Bookie Servers (B1, B2, B3)
        for (int i = 0; i < numBookies; i++) {
            startNewBookie();
        }
    }

    public BookKeeper newClient() throws Exception {
        return new BookKeeper(baseClientConf);
    }

    /**
     * Stop cluster. Also, stops all the auto recovery processes for the bookie
     * cluster, if isAutoRecoveryEnabled is true.
     *
     * @throws Exception
     */
    protected void stopBKCluster() throws Exception {
        for (BookieServer server : bs) {
            server.shutdown();
            AutoRecoveryMain autoRecovery = autoRecoveryProcesses.get(server);
            if (autoRecovery != null && isAutoRecoveryEnabled()) {
                autoRecovery.shutdown();
                log.debug("Shutdown auto recovery for bookieserver:"
                        + server.getBookieId());
            }
        }
        bs.clear();
    }

    protected void cleanupTempDirs() throws Exception {
        for (File f : tmpDirs) {
            FileUtils.deleteDirectory(f);
        }
    }

    private ServerConfiguration newServerConfiguration() throws Exception {
        File f = createTempDir("bookie", "test");

        int port;
        if (baseConf.isEnableLocalTransport() || !baseConf.getAllowEphemeralPorts()) {
            port = PortManager.nextFreePort();
        } else {
            port = 0;
        }
        return newServerConfiguration(port, f, new File[]{f});
    }

    private ClientConfiguration newClientConfiguration() {
        return new ClientConfiguration(baseConf);
    }

    private ServerConfiguration newServerConfiguration(int port, File journalDir, File[] ledgerDirs) {
        ServerConfiguration conf = new ServerConfiguration(baseConf);
        conf.setBookiePort(port);
        conf.setJournalDirName(journalDir.getPath());
        String[] ledgerDirNames = new String[ledgerDirs.length];
        for (int i = 0; i < ledgerDirs.length; i++) {
            ledgerDirNames[i] = ledgerDirs[i].getPath();
        }
        conf.setLedgerDirNames(ledgerDirNames);
        conf.setEnableTaskExecutionStats(true);
        conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        return conf;
    }

    protected void stopAllBookies() throws Exception {
        stopAllBookies(true);
    }

    protected void stopAllBookies(boolean shutdownClient) throws Exception {
        for (BookieServer server : bs) {
            server.shutdown();
        }
        bsConfs.clear();
        bs.clear();
    }

    protected void startAllBookies() throws Exception {
        for (ServerConfiguration conf : bsConfs) {
            bs.add(startBookie(conf));
        }
    }

    /**
     * Helper method to startup a new bookie server with the indicated port
     * number. Also, starts the auto recovery process, if the
     * isAutoRecoveryEnabled is set true.
     *
     * @throws IOException
     */
    public int startNewBookie()
            throws Exception {
        ServerConfiguration conf = newServerConfiguration();

        bsConfs.add(conf);
        log.info("Starting new bookie on port: {}", conf.getBookiePort());
        BookieServer server = startBookie(conf);
        bs.add(server);
        return server.getLocalAddress().getPort();
    }

    /**
     * Helper method to startup a bookie server using a configuration object.
     * Also, starts the auto recovery process if isAutoRecoveryEnabled is true.
     *
     * @param conf
     *            Server Configuration Object
     *
     */
    protected BookieServer startBookie(ServerConfiguration conf)
            throws Exception {
        BookieServer server = new BookieServer(conf, NullStatsLogger.INSTANCE, null);
        BookieId address = Bookie.getBookieId(conf);

        @Cleanup
        BookKeeperAdmin bkc = new BookKeeperAdmin(baseClientConf);

        server.start();

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
            Assert.assertTrue(server.isRunning());
        });

        log.info("New bookie '{}' has been created.", address);

        try {
            startAutoRecovery(server, conf);
        } catch (CompatibilityException ce) {
            log.error("Exception while starting AutoRecovery!", ce);
        } catch (UnavailableException ue) {
            log.error("Exception while starting AutoRecovery!", ue);
        }
        return server;
    }

    private void startAutoRecovery(BookieServer bserver,
                                   ServerConfiguration conf) throws Exception {
        if (isAutoRecoveryEnabled()) {
            AutoRecoveryMain autoRecoveryProcess = new AutoRecoveryMain(conf);
            autoRecoveryProcess.start();
            autoRecoveryProcesses.put(bserver, autoRecoveryProcess);
            log.debug("Starting Auditor Recovery for the bookie:"
                    + bserver.getBookieId());
        }
    }
}
