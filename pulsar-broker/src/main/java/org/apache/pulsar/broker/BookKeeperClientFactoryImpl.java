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

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;
import org.apache.pulsar.zookeeper.ZkIsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.ZooKeeper;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

@SuppressWarnings("deprecation")
public class BookKeeperClientFactoryImpl implements BookKeeperClientFactory {

    private final AtomicReference<ZooKeeperCache> rackawarePolicyZkCache = new AtomicReference<>();
    private final AtomicReference<ZooKeeperCache> clientIsolationZkCache = new AtomicReference<>();
    private final AtomicReference<ZooKeeperCache> zkCache = new AtomicReference<>();

    @Override
    public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient,
            Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass, Map<String, Object> properties) throws IOException {
        ClientConfiguration bkConf = createBkClientConfiguration(conf);
        if (properties != null) {
            properties.forEach((key, value) -> bkConf.setProperty(key, value));
        }
        if (ensemblePlacementPolicyClass.isPresent()) {
            setEnsemblePlacementPolicy(bkConf, conf, zkClient, ensemblePlacementPolicyClass.get());
        } else {
            setDefaultEnsemblePlacementPolicy(bkConf, conf, zkClient);
        }
        try {
            return BookKeeper.forConfig(bkConf)
                    .allocator(PulsarByteBufAllocator.DEFAULT)
                    .zk(zkClient)
                    .build();
        } catch (InterruptedException | BKException e) {
            throw new IOException(e);
        }
    }

    private ClientConfiguration createBkClientConfiguration(ServiceConfiguration conf) {
        ClientConfiguration bkConf = new ClientConfiguration();
        if (conf.getBookkeeperClientAuthenticationPlugin() != null
                && conf.getBookkeeperClientAuthenticationPlugin().trim().length() > 0) {
            bkConf.setClientAuthProviderFactoryClass(conf.getBookkeeperClientAuthenticationPlugin());
            bkConf.setProperty(conf.getBookkeeperClientAuthenticationParametersName(),
                    conf.getBookkeeperClientAuthenticationParameters());
        }

        bkConf.setThrottleValue(0);
        bkConf.setAddEntryTimeout((int) conf.getBookkeeperClientTimeoutInSeconds());
        bkConf.setReadEntryTimeout((int) conf.getBookkeeperClientTimeoutInSeconds());
        bkConf.setSpeculativeReadTimeout(conf.getBookkeeperClientSpeculativeReadTimeoutInMillis());
        bkConf.setNumChannelsPerBookie(16);
        bkConf.setUseV2WireProtocol(conf.isBookkeeperUseV2WireProtocol());
        bkConf.setEnableDigestTypeAutodetection(true);
        bkConf.setStickyReadsEnabled(conf.isBookkeeperEnableStickyReads());
        bkConf.setNettyMaxFrameSizeBytes(conf.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING);

        if (conf.isBookkeeperClientHealthCheckEnabled()) {
            bkConf.enableBookieHealthCheck();
            bkConf.setBookieHealthCheckInterval(conf.getBookkeeperHealthCheckIntervalSec(), TimeUnit.SECONDS);
            bkConf.setBookieErrorThresholdPerInterval(conf.getBookkeeperClientHealthCheckErrorThresholdPerInterval());
            bkConf.setBookieQuarantineTime((int) conf.getBookkeeperClientHealthCheckQuarantineTimeInSeconds(),
                    TimeUnit.SECONDS);
        }

        bkConf.setReorderReadSequenceEnabled(conf.isBookkeeperClientReorderReadSequenceEnabled());

        return bkConf;
    }

    private void setDefaultEnsemblePlacementPolicy(ClientConfiguration bkConf, ServiceConfiguration conf,
            ZooKeeper zkClient) {
        if (conf.isBookkeeperClientRackawarePolicyEnabled() || conf.isBookkeeperClientRegionawarePolicyEnabled()) {
            if (conf.isBookkeeperClientRegionawarePolicyEnabled()) {
                bkConf.setEnsemblePlacementPolicy(RegionAwareEnsemblePlacementPolicy.class);
            } else {
                bkConf.setEnsemblePlacementPolicy(RackawareEnsemblePlacementPolicy.class);
            }
            bkConf.setProperty(RackawareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS,
                    ZkBookieRackAffinityMapping.class.getName());

            ZooKeeperCache zkc = new ZooKeeperCache(zkClient, conf.getZooKeeperOperationTimeoutSeconds()) {
            };
            if (!rackawarePolicyZkCache.compareAndSet(null, zkc)) {
                zkc.stop();
            }

            bkConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, this.rackawarePolicyZkCache.get());
        }

        if (conf.getBookkeeperClientIsolationGroups() != null && !conf.getBookkeeperClientIsolationGroups().isEmpty()) {
            bkConf.setEnsemblePlacementPolicy(ZkIsolatedBookieEnsemblePlacementPolicy.class);
            bkConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientIsolationGroups());
            bkConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientSecondaryIsolationGroups());
            if (bkConf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE) == null) {
                ZooKeeperCache zkc = new ZooKeeperCache(zkClient, conf.getZooKeeperOperationTimeoutSeconds()) {
                };

                if (!clientIsolationZkCache.compareAndSet(null, zkc)) {
                    zkc.stop();
                }
                bkConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, this.clientIsolationZkCache.get());
            }
        }
    }

    private void setEnsemblePlacementPolicy(ClientConfiguration bkConf, ServiceConfiguration conf, ZooKeeper zkClient,
            Class<? extends EnsemblePlacementPolicy> policyClass) {
        bkConf.setEnsemblePlacementPolicy(policyClass);
        if (bkConf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE) == null) {
            ZooKeeperCache zkc = new ZooKeeperCache(zkClient, conf.getZooKeeperOperationTimeoutSeconds()) {
            };
            if (!zkCache.compareAndSet(null, zkc)) {
                zkc.stop();
            }
            bkConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, this.zkCache.get());
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