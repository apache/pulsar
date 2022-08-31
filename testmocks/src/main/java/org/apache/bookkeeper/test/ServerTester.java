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
 * This file is derived from BookKeeperClusterTestCase from Apache BookKeeper
 * http://bookkeeper.apache.org
 */

package org.apache.bookkeeper.test;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.LegacyCookieValidation;
import org.apache.bookkeeper.bookie.ReadOnlyBookie;
import org.apache.bookkeeper.bookie.UncleanShutdownDetection;
import org.apache.bookkeeper.bookie.UncleanShutdownDetectionImpl;
import org.apache.bookkeeper.client.TestStatsProvider;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.Auditor;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.replication.ReplicationWorker;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to encapsulate all the test objects.
 */
public class ServerTester {
    static final Logger LOG = LoggerFactory.getLogger(ServerTester.class);

    /**
     * Mock implementation of UncleanShutdownDetection.
     */
    public static class MockUncleanShutdownDetection implements UncleanShutdownDetection {

        private boolean startRegistered;
        private boolean shutdownRegistered;

        @Override
        public void registerStartUp() {
            startRegistered = true;
        }

        @Override
        public void registerCleanShutdown() {
            shutdownRegistered = true;
        }

        @Override
        public boolean lastShutdownWasUnclean() {
            return startRegistered && !shutdownRegistered;
        }

        public boolean getStartRegistered() {
            return startRegistered;
        }

        public boolean getShutdownRegistered() {
            return shutdownRegistered;
        }
    }

    private final ServerConfiguration conf;
    private final TestStatsProvider provider;
    private final Bookie bookie;
    private final BookieServer server;
    private final BookieSocketAddress address;
    private final MetadataBookieDriver metadataDriver;
    private final RegistrationManager registrationManager;
    private final LedgerManagerFactory lmFactory;
    private final LedgerManager ledgerManager;
    private final LedgerStorage storage;

    public AutoRecoveryMain autoRecovery;
    private final ByteBufAllocatorWithOomHandler allocator = BookieResources
            .createAllocator((new ServerConfiguration()).setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap));

    public ServerTester(ServerConfiguration conf) throws Exception {
        this.conf = conf;
        provider = new TestStatsProvider();

        StatsLogger rootStatsLogger = provider.getStatsLogger("");
        StatsLogger bookieStats = rootStatsLogger.scope(BOOKIE_SCOPE);

        metadataDriver = BookieResources.createMetadataDriver(conf, bookieStats);
        registrationManager = metadataDriver.createRegistrationManager();
        lmFactory = metadataDriver.getLedgerManagerFactory();
        ledgerManager = lmFactory.newLedgerManager();

        LegacyCookieValidation cookieValidation = new LegacyCookieValidation(
                conf, registrationManager);
        cookieValidation.checkCookies(Main.storageDirectoriesFromConf(conf));

        DiskChecker diskChecker = BookieResources.createDiskChecker(conf);
        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                conf, diskChecker, bookieStats.scope(LD_LEDGER_SCOPE));
        LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                conf, diskChecker, bookieStats.scope(LD_INDEX_SCOPE), ledgerDirsManager);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);

        storage = BookieResources.createLedgerStorage(
                conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                bookieStats, allocator);

        if (conf.isForceReadOnlyBookie()) {
            bookie = new ReadOnlyBookie(conf, registrationManager, storage,
                    diskChecker, ledgerDirsManager, indexDirsManager,
                    bookieStats, allocator, BookieServiceInfo.NO_INFO);
        } else {
            bookie = new BookieImpl(conf, registrationManager, storage,
                    diskChecker, ledgerDirsManager, indexDirsManager,
                    bookieStats, allocator, BookieServiceInfo.NO_INFO);
        }
        server = new BookieServer(conf, bookie, rootStatsLogger, allocator,
                uncleanShutdownDetection);
        address = BookieImpl.getBookieAddress(conf);

        autoRecovery = null;
    }

    public ServerTester(ServerConfiguration conf, Bookie b) throws Exception {
        this.conf = conf;
        provider = new TestStatsProvider();

        metadataDriver = null;
        registrationManager = null;
        ledgerManager = null;
        lmFactory = null;
        storage = null;

        bookie = b;
        server = new BookieServer(conf, b, provider.getStatsLogger(""),
                allocator, new MockUncleanShutdownDetection());
        address = BookieImpl.getBookieAddress(conf);

        autoRecovery = null;
    }

    public void startAutoRecovery() throws Exception {
        LOG.debug("Starting Auditor Recovery for the bookie: {}", address);
        autoRecovery = new AutoRecoveryMain(conf);
        autoRecovery.start();
    }

    public void stopAutoRecovery() {
        if (autoRecovery != null) {
            LOG.debug("Shutdown Auditor Recovery for the bookie: {}", address);
            autoRecovery.shutdown();
        }
    }

    public Auditor getAuditor() {
        if (autoRecovery != null) {
            return autoRecovery.getAuditor();
        } else {
            return null;
        }
    }

    public ReplicationWorker getReplicationWorker() {
        if (autoRecovery != null) {
            return autoRecovery.getReplicationWorker();
        } else {
            return null;
        }
    }

    public ServerConfiguration getConfiguration() {
        return conf;
    }

    public BookieServer getServer() {
        return server;
    }

    public TestStatsProvider getStatsProvider() {
        return provider;
    }

    public BookieSocketAddress getAddress() {
        return address;
    }

    public void shutdown() throws Exception {
        server.shutdown();

        if (ledgerManager != null) {
            ledgerManager.close();
        }
        if (lmFactory != null) {
            lmFactory.close();
        }
        if (registrationManager != null) {
            registrationManager.close();
        }
        if (metadataDriver != null) {
            metadataDriver.close();
        }

        if (autoRecovery != null) {
            LOG.debug("Shutdown auto recovery for bookieserver: {}", address);
            autoRecovery.shutdown();
        }
    }
}