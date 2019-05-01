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
package org.apache.pulsar.sql.presto;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.OffloaderUtils;
import org.apache.bookkeeper.mledger.offload.Offloaders;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class PulsarConnectorCache {

    private static final Logger log = Logger.get(PulsarConnectorCache.class);

    private static PulsarConnectorCache instance;

    private final ManagedLedgerFactory managedLedgerFactory;

    private final StatsProvider statsProvider;
    private OrderedScheduler offloaderScheduler;
    private Offloaders offloaderManager;
    private LedgerOffloader offloader;

    private static final String OFFLOADERS_DIRECTOR = "offloadersDirectory";
    private static final String MANAGED_LEDGER_OFFLOAD_DRIVER = "managedLedgerOffloadDriver";
    private static final String MANAGED_LEDGER_OFFLOAD_MAX_THREADS = "managedLedgerOffloadMaxThreads";


    private PulsarConnectorCache(PulsarConnectorConfig pulsarConnectorConfig) throws Exception {
        this.managedLedgerFactory = initManagedLedgerFactory(pulsarConnectorConfig);
        this.statsProvider = PulsarConnectorUtils.createInstance(pulsarConnectorConfig.getStatsProvider(),
                StatsProvider.class, getClass().getClassLoader());

        // start stats provider
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        pulsarConnectorConfig.getStatsProviderConfigs().forEach((key, value) -> clientConfiguration.setProperty(key, value));

        this.statsProvider.start(clientConfiguration);

        this.offloader = initManagedLedgerOffloader(pulsarConnectorConfig);
    }

    public static PulsarConnectorCache getConnectorCache(PulsarConnectorConfig pulsarConnectorConfig) throws Exception {
        synchronized (PulsarConnectorCache.class) {
            if (instance == null) {
                instance = new PulsarConnectorCache(pulsarConnectorConfig);
            }
        }
        return instance;
    }

    private static ManagedLedgerFactory initManagedLedgerFactory(PulsarConnectorConfig pulsarConnectorConfig) throws Exception {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration()
                .setZkServers(pulsarConnectorConfig.getZookeeperUri())
                .setClientTcpNoDelay(false)
                .setUseV2WireProtocol(true)
                .setStickyReadsEnabled(true)
                .setReadEntryTimeout(60);
        return new ManagedLedgerFactoryImpl(bkClientConfiguration);
    }

    public ManagedLedgerConfig getManagedLedgerConfig() {

        return new ManagedLedgerConfig()
                .setLedgerOffloader(this.offloader);
    }

    private synchronized OrderedScheduler getOffloaderScheduler(PulsarConnectorConfig pulsarConnectorConfig) {
        if (this.offloaderScheduler == null) {
            this.offloaderScheduler = OrderedScheduler.newSchedulerBuilder()
                    .numThreads(pulsarConnectorConfig.getManagedLedgerOffloadMaxThreads())
                    .name("pulsar-offloader").build();
        }
        return this.offloaderScheduler;
    }

    private LedgerOffloader initManagedLedgerOffloader(PulsarConnectorConfig conf) {

        try {
            if (StringUtils.isNotBlank(conf.getManagedLedgerOffloadDriver())) {
                checkNotNull(conf.getOffloadersDirectory(),
                        "Offloader driver is configured to be '%s' but no offloaders directory is configured.",
                        conf.getManagedLedgerOffloadDriver());
                this.offloaderManager = OffloaderUtils.searchForOffloaders(conf.getOffloadersDirectory());
                LedgerOffloaderFactory offloaderFactory = this.offloaderManager.getOffloaderFactory(
                        conf.getManagedLedgerOffloadDriver());

                Map<String, String> offloaderProperties = conf.getOffloaderProperties();
                offloaderProperties.put(OFFLOADERS_DIRECTOR, conf.getOffloadersDirectory());
                offloaderProperties.put(MANAGED_LEDGER_OFFLOAD_DRIVER, conf.getManagedLedgerOffloadDriver());
                offloaderProperties.put(MANAGED_LEDGER_OFFLOAD_MAX_THREADS, String.valueOf(conf.getManagedLedgerOffloadMaxThreads()));

                try {
                    return offloaderFactory.create(
                            PulsarConnectorUtils.getProperties(offloaderProperties),
                            ImmutableMap.of(
                                    LedgerOffloader.METADATA_SOFTWARE_VERSION_KEY.toLowerCase(), PulsarVersion.getVersion(),
                                    LedgerOffloader.METADATA_SOFTWARE_GITSHA_KEY.toLowerCase(), PulsarVersion.getGitSha()
                            ),
                            getOffloaderScheduler(conf));
                } catch (IOException ioe) {
                    log.error("Failed to create offloader: ", ioe);
                    throw new RuntimeException(ioe.getMessage(), ioe.getCause());
                }
            } else {
                log.info("No ledger offloader configured, using NULL instance");
                return NullLedgerOffloader.INSTANCE;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public ManagedLedgerFactory getManagedLedgerFactory() {
        return managedLedgerFactory;
    }

    public StatsProvider getStatsProvider() {
        return statsProvider;
    }

    public static void shutdown() throws Exception {
        synchronized (PulsarConnectorCache.class) {
            if (instance != null) {
                instance.statsProvider.stop();
                instance.managedLedgerFactory.shutdown();
                instance.offloaderScheduler.shutdown();
                instance.offloaderManager.close();
                instance = null;
            }
        }
    }
}
