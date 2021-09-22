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

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.Offloaders;
import org.apache.bookkeeper.mledger.offload.OffloadersCache;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * Implementation of a cache for the Pulsar connector.
 */
public class PulsarConnectorCache {

    private static final Logger log = Logger.get(PulsarConnectorCache.class);

    @VisibleForTesting
    static PulsarConnectorCache instance;

    private final MetadataStoreExtended metadataStore;
    private final ManagedLedgerFactory managedLedgerFactory;

    private final StatsProvider statsProvider;
    private OrderedScheduler offloaderScheduler;
    private OffloadersCache offloadersCache = new OffloadersCache();
    private LedgerOffloader defaultOffloader;
    private Map<NamespaceName, LedgerOffloader> offloaderMap = new ConcurrentHashMap<>();

    private static final String OFFLOADERS_DIRECTOR = "offloadersDirectory";
    private static final String MANAGED_LEDGER_OFFLOAD_DRIVER = "managedLedgerOffloadDriver";
    private static final String MANAGED_LEDGER_OFFLOAD_MAX_THREADS = "managedLedgerOffloadMaxThreads";


    private PulsarConnectorCache(PulsarConnectorConfig pulsarConnectorConfig) throws Exception {
        this.metadataStore = MetadataStoreExtended.create(pulsarConnectorConfig.getZookeeperUri(),
                MetadataStoreConfig.builder().build());
        this.managedLedgerFactory = initManagedLedgerFactory(pulsarConnectorConfig);
        this.statsProvider = PulsarConnectorUtils.createInstance(pulsarConnectorConfig.getStatsProvider(),
                StatsProvider.class, getClass().getClassLoader());

        // start stats provider
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        pulsarConnectorConfig.getStatsProviderConfigs().forEach(clientConfiguration::setProperty);

        this.statsProvider.start(clientConfiguration);

        this.defaultOffloader = initManagedLedgerOffloader(
                pulsarConnectorConfig.getOffloadPolices(), pulsarConnectorConfig);
    }

    public static PulsarConnectorCache getConnectorCache(PulsarConnectorConfig pulsarConnectorConfig) throws Exception {
        synchronized (PulsarConnectorCache.class) {
            if (instance == null) {
                instance = new PulsarConnectorCache(pulsarConnectorConfig);
            }
        }
        return instance;
    }

    private ManagedLedgerFactory initManagedLedgerFactory(PulsarConnectorConfig pulsarConnectorConfig)
        throws Exception {
        ClientConfiguration bkClientConfiguration = new ClientConfiguration()
            .setZkServers(pulsarConnectorConfig.getZookeeperUri())
            .setMetadataServiceUri("zk://" + pulsarConnectorConfig.getZookeeperUri()
                .replace(",", ";") + "/ledgers")
            .setClientTcpNoDelay(false)
            .setUseV2WireProtocol(pulsarConnectorConfig.getBookkeeperUseV2Protocol())
            .setExplictLacInterval(pulsarConnectorConfig.getBookkeeperExplicitInterval())
            .setStickyReadsEnabled(false)
            .setReadEntryTimeout(60)
            .setThrottleValue(pulsarConnectorConfig.getBookkeeperThrottleValue())
            .setNumIOThreads(pulsarConnectorConfig.getBookkeeperNumIOThreads())
            .setNumWorkerThreads(pulsarConnectorConfig.getBookkeeperNumWorkerThreads());

        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        managedLedgerFactoryConfig.setMaxCacheSize(pulsarConnectorConfig.getManagedLedgerCacheSizeMB());
        managedLedgerFactoryConfig.setNumManagedLedgerSchedulerThreads(
                pulsarConnectorConfig.getManagedLedgerNumSchedulerThreads());
        return new ManagedLedgerFactoryImpl(metadataStore, bkClientConfiguration, managedLedgerFactoryConfig);
    }

    public ManagedLedgerConfig getManagedLedgerConfig(NamespaceName namespaceName, OffloadPoliciesImpl offloadPolicies,
                                                      PulsarConnectorConfig pulsarConnectorConfig) {
        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        if (offloadPolicies == null) {
            managedLedgerConfig.setLedgerOffloader(this.defaultOffloader);
        } else {
            LedgerOffloader ledgerOffloader = offloaderMap.compute(namespaceName,
                    (ns, offloader) -> {
                        if (offloader != null && Objects.equals(offloader.getOffloadPolicies(), offloadPolicies)) {
                            return offloader;
                        } else {
                            if (offloader != null) {
                                offloader.close();
                            }
                            return initManagedLedgerOffloader(offloadPolicies, pulsarConnectorConfig);
                        }
                    });
            managedLedgerConfig.setLedgerOffloader(ledgerOffloader);
        }
        return managedLedgerConfig;
    }

    private synchronized OrderedScheduler getOffloaderScheduler(OffloadPoliciesImpl offloadPolicies) {
        if (this.offloaderScheduler == null) {
            this.offloaderScheduler = OrderedScheduler.newSchedulerBuilder()
                    .numThreads(offloadPolicies.getManagedLedgerOffloadMaxThreads())
                    .name("pulsar-offloader").build();
        }
        return this.offloaderScheduler;
    }

    private LedgerOffloader initManagedLedgerOffloader(OffloadPoliciesImpl offloadPolicies,
                                                       PulsarConnectorConfig pulsarConnectorConfig) {

        try {
            if (StringUtils.isNotBlank(offloadPolicies.getManagedLedgerOffloadDriver())) {
                checkNotNull(offloadPolicies.getOffloadersDirectory(),
                        "Offloader driver is configured to be '%s' but no offloaders directory is configured.",
                        offloadPolicies.getManagedLedgerOffloadDriver());
                Offloaders offloaders = offloadersCache.getOrLoadOffloaders(offloadPolicies.getOffloadersDirectory(),
                        pulsarConnectorConfig.getNarExtractionDirectory());
                LedgerOffloaderFactory offloaderFactory = offloaders.getOffloaderFactory(
                        offloadPolicies.getManagedLedgerOffloadDriver());

                try {
                    return offloaderFactory.create(
                        offloadPolicies,
                        ImmutableMap.of(
                            LedgerOffloader.METADATA_SOFTWARE_VERSION_KEY.toLowerCase(), PulsarVersion.getVersion(),
                            LedgerOffloader.METADATA_SOFTWARE_GITSHA_KEY.toLowerCase(), PulsarVersion.getGitSha()
                        ),
                        getOffloaderScheduler(offloadPolicies));
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
                instance.metadataStore.close();
                instance.offloaderScheduler.shutdown();
                instance.offloadersCache.close();
            }
        }
    }
}
