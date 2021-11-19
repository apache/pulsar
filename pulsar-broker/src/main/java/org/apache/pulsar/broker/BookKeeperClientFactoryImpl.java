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
package org.apache.pulsar.broker;

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_ENABLE_VALIDATION;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_MINIMUM_REGIONS_FOR_DURABILITY;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_REGIONS_TO_WRITE;
import static org.apache.bookkeeper.net.CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY;
import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;
import org.apache.pulsar.zookeeper.ZkIsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.ZooKeeper;

@SuppressWarnings("deprecation")
@Slf4j
public class BookKeeperClientFactoryImpl implements BookKeeperClientFactory {

    private final AtomicReference<ZooKeeperCache> rackawarePolicyZkCache = new AtomicReference<>();
    private final AtomicReference<ZooKeeperCache> clientIsolationZkCache = new AtomicReference<>();
    private final AtomicReference<ZooKeeperCache> zkCache = new AtomicReference<>();

    @Override
    public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient, EventLoopGroup eventLoopGroup,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties) throws IOException {
        return create(conf, zkClient, eventLoopGroup, ensemblePlacementPolicyClass, properties,
                NullStatsLogger.INSTANCE);
    }

    @Override
    public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient, EventLoopGroup eventLoopGroup,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties, StatsLogger statsLogger) throws IOException {
        ClientConfiguration bkConf = createBkClientConfiguration(conf);
        if (properties != null) {
            properties.forEach((key, value) -> bkConf.setProperty(key, value));
        }
        if (ensemblePlacementPolicyClass.isPresent()) {
            setEnsemblePlacementPolicy(bkConf, conf, zkClient, ensemblePlacementPolicyClass.get());
        } else {
            setDefaultEnsemblePlacementPolicy(rackawarePolicyZkCache, clientIsolationZkCache, bkConf, conf, zkClient);
        }
        try {
            return BookKeeper.forConfig(bkConf)
                    .allocator(PulsarByteBufAllocator.DEFAULT)
                    .eventLoopGroup(eventLoopGroup)
                    .statsLogger(statsLogger)
                    .build();
        } catch (InterruptedException | BKException e) {
            throw new IOException(e);
        }
    }

    @VisibleForTesting
    ClientConfiguration createBkClientConfiguration(ServiceConfiguration conf) {
        ClientConfiguration bkConf = new ClientConfiguration();
        if (conf.getBookkeeperClientAuthenticationPlugin() != null
                && conf.getBookkeeperClientAuthenticationPlugin().trim().length() > 0) {
            bkConf.setClientAuthProviderFactoryClass(conf.getBookkeeperClientAuthenticationPlugin());
            bkConf.setProperty(conf.getBookkeeperClientAuthenticationParametersName(),
                    conf.getBookkeeperClientAuthenticationParameters());
        }

        if (conf.isBookkeeperTLSClientAuthentication()) {
            bkConf.setTLSClientAuthentication(true);
            bkConf.setTLSCertificatePath(conf.getBookkeeperTLSCertificateFilePath());
            bkConf.setTLSKeyStore(conf.getBookkeeperTLSKeyFilePath());
            bkConf.setTLSKeyStoreType(conf.getBookkeeperTLSKeyFileType());
            bkConf.setTLSKeyStorePasswordPath(conf.getBookkeeperTLSKeyStorePasswordPath());
            bkConf.setTLSProviderFactoryClass(conf.getBookkeeperTLSProviderFactoryClass());
            bkConf.setTLSTrustStore(conf.getBookkeeperTLSTrustCertsFilePath());
            bkConf.setTLSTrustStoreType(conf.getBookkeeperTLSTrustCertTypes());
            bkConf.setTLSTrustStorePasswordPath(conf.getBookkeeperTLSTrustStorePasswordPath());
            bkConf.setTLSCertFilesRefreshDurationSeconds(conf.getBookkeeperTlsCertFilesRefreshDurationSeconds());
        }

        bkConf.setBusyWaitEnabled(conf.isEnableBusyWait());
        bkConf.setNumWorkerThreads(conf.getBookkeeperClientNumWorkerThreads());
        bkConf.setThrottleValue(conf.getBookkeeperClientThrottleValue());
        bkConf.setAddEntryTimeout((int) conf.getBookkeeperClientTimeoutInSeconds());
        bkConf.setReadEntryTimeout((int) conf.getBookkeeperClientTimeoutInSeconds());
        bkConf.setSpeculativeReadTimeout(conf.getBookkeeperClientSpeculativeReadTimeoutInMillis());
        bkConf.setNumChannelsPerBookie(conf.getBookkeeperNumberOfChannelsPerBookie());
        bkConf.setUseV2WireProtocol(conf.isBookkeeperUseV2WireProtocol());
        bkConf.setEnableDigestTypeAutodetection(true);
        bkConf.setStickyReadsEnabled(conf.isBookkeeperEnableStickyReads());
        bkConf.setNettyMaxFrameSizeBytes(conf.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING);
        bkConf.setDiskWeightBasedPlacementEnabled(conf.isBookkeeperDiskWeightBasedPlacementEnabled());

        if (StringUtils.isNotBlank(conf.getBookkeeperMetadataServiceUri())) {
            bkConf.setMetadataServiceUri(conf.getBookkeeperMetadataServiceUri());
        } else {
            String metadataServiceUri = PulsarService.bookieMetadataServiceUri(conf);
            bkConf.setMetadataServiceUri(metadataServiceUri);
        }

        if (conf.isBookkeeperClientHealthCheckEnabled()) {
            bkConf.enableBookieHealthCheck();
            bkConf.setBookieHealthCheckInterval(conf.getBookkeeperHealthCheckIntervalSec(), TimeUnit.SECONDS);
            bkConf.setBookieErrorThresholdPerInterval(conf.getBookkeeperClientHealthCheckErrorThresholdPerInterval());
            bkConf.setBookieQuarantineTime((int) conf.getBookkeeperClientHealthCheckQuarantineTimeInSeconds(),
                    TimeUnit.SECONDS);
            bkConf.setBookieQuarantineRatio(conf.getBookkeeperClientQuarantineRatio());
        }

        bkConf.setReorderReadSequenceEnabled(conf.isBookkeeperClientReorderReadSequenceEnabled());
        bkConf.setExplictLacInterval(conf.getBookkeeperExplicitLacIntervalInMills());
        bkConf.setGetBookieInfoIntervalSeconds(
                conf.getBookkeeperClientGetBookieInfoIntervalSeconds(), TimeUnit.SECONDS);
        bkConf.setGetBookieInfoRetryIntervalSeconds(
                conf.getBookkeeperClientGetBookieInfoRetryIntervalSeconds(), TimeUnit.SECONDS);
        Properties allProps = conf.getProperties();
        allProps.forEach((key, value) -> {
            String sKey = key.toString();
            if (sKey.startsWith("bookkeeper_") && value != null) {
                String bkExtraConfigKey = sKey.substring(11);
                log.info("Extra BookKeeper client configuration {}, setting {}={}", sKey, bkExtraConfigKey, value);
                bkConf.setProperty(bkExtraConfigKey, value);
            }
        });
        return bkConf;
    }

    public static void setDefaultEnsemblePlacementPolicy(
            AtomicReference<ZooKeeperCache> rackawarePolicyZkCache,
            AtomicReference<ZooKeeperCache> clientIsolationZkCache,
            ClientConfiguration bkConf,
            ServiceConfiguration conf,
            ZooKeeper zkClient
    ) {
        if (conf.isBookkeeperClientRackawarePolicyEnabled() || conf.isBookkeeperClientRegionawarePolicyEnabled()) {
            if (conf.isBookkeeperClientRegionawarePolicyEnabled()) {
                bkConf.setEnsemblePlacementPolicy(RegionAwareEnsemblePlacementPolicy.class);

                bkConf.setProperty(
                        REPP_ENABLE_VALIDATION,
                        conf.getProperties().getProperty(REPP_ENABLE_VALIDATION, "true")
                );
                bkConf.setProperty(
                        REPP_REGIONS_TO_WRITE,
                        conf.getProperties().getProperty(REPP_REGIONS_TO_WRITE, null)
                );
                bkConf.setProperty(
                        REPP_MINIMUM_REGIONS_FOR_DURABILITY,
                        conf.getProperties().getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, "2")
                );
                bkConf.setProperty(
                        REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE,
                        conf.getProperties().getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, "true")
                );
            } else {
                bkConf.setEnsemblePlacementPolicy(RackawareEnsemblePlacementPolicy.class);
            }

            bkConf.setMinNumRacksPerWriteQuorum(conf.getBookkeeperClientMinNumRacksPerWriteQuorum());
            bkConf.setEnforceMinNumRacksPerWriteQuorum(conf.isBookkeeperClientEnforceMinNumRacksPerWriteQuorum());

            bkConf.setProperty(REPP_DNS_RESOLVER_CLASS,
                    conf.getProperties().getProperty(
                            REPP_DNS_RESOLVER_CLASS,
                            ZkBookieRackAffinityMapping.class.getName()));

            bkConf.setProperty(NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                conf.getProperties().getProperty(
                    NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                    ""));

            ZooKeeperCache zkc = new ZooKeeperCache("bookies-racks", zkClient,
                    conf.getZooKeeperOperationTimeoutSeconds()) {
            };
            if (!rackawarePolicyZkCache.compareAndSet(null, zkc)) {
                zkc.stop();
            }

            bkConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, rackawarePolicyZkCache.get());
        }

        if (conf.getBookkeeperClientIsolationGroups() != null && !conf.getBookkeeperClientIsolationGroups().isEmpty()) {
            bkConf.setEnsemblePlacementPolicy(ZkIsolatedBookieEnsemblePlacementPolicy.class);
            bkConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientIsolationGroups());
            bkConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientSecondaryIsolationGroups());
            if (bkConf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE) == null) {
                ZooKeeperCache zkc = new ZooKeeperCache("bookies-isolation", zkClient,
                        conf.getZooKeeperOperationTimeoutSeconds()) {
                };

                if (!clientIsolationZkCache.compareAndSet(null, zkc)) {
                    zkc.stop();
                }
                bkConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, clientIsolationZkCache.get());
            }
        }
    }

    private void setEnsemblePlacementPolicy(ClientConfiguration bkConf, ServiceConfiguration conf, ZooKeeper zkClient,
                                            Class<? extends EnsemblePlacementPolicy> policyClass) {
        bkConf.setEnsemblePlacementPolicy(policyClass);
        if (bkConf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE) == null) {
            ZooKeeperCache zkc = new ZooKeeperCache("bookies-rackaware", zkClient,
                    conf.getZooKeeperOperationTimeoutSeconds()) {
            };
            if (!zkCache.compareAndSet(null, zkc)) {
                zkc.stop();
            }
            bkConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, this.zkCache.get());
        }
        if (conf.isBookkeeperClientRackawarePolicyEnabled() || conf.isBookkeeperClientRegionawarePolicyEnabled()) {
            bkConf.setProperty(REPP_DNS_RESOLVER_CLASS, conf.getProperties().getProperty(REPP_DNS_RESOLVER_CLASS,
                    ZkBookieRackAffinityMapping.class.getName()));

            bkConf.setProperty(NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                conf.getProperties().getProperty(
                    NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                    ""));
        }
    }

    public void close() {
        if (this.rackawarePolicyZkCache.get() != null) {
            this.rackawarePolicyZkCache.get().stop();
        }
        if (this.clientIsolationZkCache.get() != null) {
            this.clientIsolationZkCache.get().stop();
        }
        if (this.zkCache.get() != null) {
            this.zkCache.get().stop();
        }
    }
}
