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
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.client.internal.PropertiesUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.bookkeeper.AbstractMetadataDriver;
import org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver;

@SuppressWarnings("deprecation")
@Slf4j
public class BookKeeperClientFactoryImpl implements BookKeeperClientFactory {

    @Override
    public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                             EventLoopGroup eventLoopGroup,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties) throws IOException {
        return create(conf, store, eventLoopGroup, ensemblePlacementPolicyClass, properties,
                NullStatsLogger.INSTANCE);
    }

    @Override
    public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                             EventLoopGroup eventLoopGroup,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties, StatsLogger statsLogger) throws IOException {
        MetadataDrivers.registerClientDriver("metadata-store", PulsarMetadataClientDriver.class);

        ClientConfiguration bkConf = createBkClientConfiguration(store, conf);
        if (properties != null) {
            properties.forEach((key, value) -> bkConf.setProperty(key, value));
        }
        if (ensemblePlacementPolicyClass.isPresent()) {
            setEnsemblePlacementPolicy(bkConf, conf, store, ensemblePlacementPolicyClass.get());
        } else {
            setDefaultEnsemblePlacementPolicy(bkConf, conf, store);
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
    ClientConfiguration createBkClientConfiguration(MetadataStoreExtended store, ServiceConfiguration conf) {
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

        bkConf.setMetadataServiceUri(conf.getBookkeeperMetadataStoreUrl());

        if (!conf.isBookkeeperMetadataStoreSeparated()) {
            // If we're connecting to the same metadata service, with same config, then
            // let's share the MetadataStore instance
            bkConf.setProperty(AbstractMetadataDriver.METADATA_STORE_INSTANCE, store);
        }

        if (conf.isBookkeeperClientHealthCheckEnabled()) {
            bkConf.enableBookieHealthCheck();
            bkConf.setBookieHealthCheckInterval((int) conf.getBookkeeperClientHealthCheckIntervalSeconds(),
                    TimeUnit.SECONDS);
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
        PropertiesUtils.filterAndMapProperties(conf.getProperties(), "bookkeeper_")
                .forEach((key, value) -> {
                    log.info("Applying BookKeeper client configuration setting {}={}", key, value);
                    bkConf.setProperty(key, value);
                });
        return bkConf;
    }

    static void setDefaultEnsemblePlacementPolicy(
            ClientConfiguration bkConf,
            ServiceConfiguration conf,
            MetadataStore store
    ) {
        bkConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);

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
                            BookieRackAffinityMapping.class.getName()));

            bkConf.setProperty(NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                conf.getProperties().getProperty(
                    NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                    ""));
        }

        if (conf.getBookkeeperClientIsolationGroups() != null && !conf.getBookkeeperClientIsolationGroups().isEmpty()) {
            bkConf.setEnsemblePlacementPolicy(IsolatedBookieEnsemblePlacementPolicy.class);
            bkConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientIsolationGroups());
            bkConf.setProperty(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientSecondaryIsolationGroups());
        }
    }

    static void setEnsemblePlacementPolicy(ClientConfiguration bkConf, ServiceConfiguration conf, MetadataStore store,
                                            Class<? extends EnsemblePlacementPolicy> policyClass) {
        bkConf.setEnsemblePlacementPolicy(policyClass);
        bkConf.setProperty(BookieRackAffinityMapping.METADATA_STORE_INSTANCE, store);
        if (conf.isBookkeeperClientRackawarePolicyEnabled() || conf.isBookkeeperClientRegionawarePolicyEnabled()) {
            bkConf.setProperty(REPP_DNS_RESOLVER_CLASS, conf.getProperties().getProperty(REPP_DNS_RESOLVER_CLASS,
                    BookieRackAffinityMapping.class.getName()));

            bkConf.setMinNumRacksPerWriteQuorum(conf.getBookkeeperClientMinNumRacksPerWriteQuorum());
            bkConf.setEnforceMinNumRacksPerWriteQuorum(conf.isBookkeeperClientEnforceMinNumRacksPerWriteQuorum());

            bkConf.setProperty(NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                conf.getProperties().getProperty(
                    NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                    ""));
        }
    }

    @Override
    public void close() {
        // Nothing to do
    }
}
