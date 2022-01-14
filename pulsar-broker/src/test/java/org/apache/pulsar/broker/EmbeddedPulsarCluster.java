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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;


public class EmbeddedPulsarCluster implements AutoCloseable {

    private static final String CLUSTER_NAME = "embedded";

    @Builder.Default
    private int numBrokers = 1;

    @Builder.Default
    private int numBookies = 1;

    private final String metadataStoreUrl;

    private final BKCluster bkCluster;

    private final List<PulsarService> brokers = new ArrayList<>();

    private final String serviceUrl;
    private final String adminUrl;

    private final PulsarAdmin admin;

    private class EmbeddedPulsarService extends PulsarService {
        EmbeddedPulsarService(ServiceConfiguration conf) {
            super(conf);
        }

        @Override
        public MetadataStoreExtended createLocalMetadataStore() throws MetadataStoreException {
            return bkCluster.getStore();
        }

        @Override
        protected void closeLocalMetadataStore() {
            // Do nothing as the store instance is managed by BKCluster
        }
    }

    @Builder
    private EmbeddedPulsarCluster(int numBrokers, int numBookies, String metadataStoreUrl) throws Exception {
        this.numBrokers = numBrokers;
        this.numBookies = numBookies;
        this.metadataStoreUrl = metadataStoreUrl;
        this.bkCluster = new BKCluster(metadataStoreUrl, numBookies);

        for (int i = 0; i < numBrokers; i++) {
            PulsarService s = new EmbeddedPulsarService(getConf());
            s.start();
            brokers.add(s);
        }

        this.serviceUrl = brokers.stream().map(ps -> ps.getBrokerServiceUrl()).collect(Collectors.joining(","));
        this.adminUrl = brokers.stream().map(ps -> ps.getWebServiceAddress()).collect(Collectors.joining(","));

        this.admin = PulsarAdmin.builder()
                .serviceHttpUrl(adminUrl)
                .build();

        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl(serviceUrl).build());
        admin.tenants().createTenant("public",
                TenantInfo.builder().allowedClusters(Collections.singleton(CLUSTER_NAME)).build());
        admin.namespaces().createNamespace("public/default", Collections.singleton(CLUSTER_NAME));
    }

    @Override
    public void close() throws Exception {
        admin.close();

        for (PulsarService s : brokers) {
            s.close();
        }

        bkCluster.close();
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    private ServiceConfiguration getConf() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setAdvertisedAddress("localhost");
        conf.setClusterName(CLUSTER_NAME);
        conf.setManagedLedgerCacheSizeMB(8);
        conf.setDefaultNumberOfNamespaceBundles(1);
        conf.setMetadataStoreUrl(metadataStoreUrl);
        conf.setBrokerShutdownTimeoutMs(0L);
        conf.setBrokerServicePort(Optional.of(0));
        conf.setWebServicePort(Optional.of(0));
        conf.setNumExecutorThreadPoolSize(1);
        conf.setNumCacheExecutorThreadPoolSize(1);
        conf.setNumWorkerThreadsForNonPersistentTopic(1);
        conf.setNumIOThreads(1);
        conf.setNumOrderedExecutorThreads(1);
        conf.setBookkeeperClientNumWorkerThreads(1);
        conf.setBookkeeperNumberOfChannelsPerBookie(1);
        conf.setManagedLedgerNumSchedulerThreads(1);
        conf.setManagedLedgerNumWorkerThreads(1);
        conf.setWebSocketNumIoThreads(1);
        conf.setNumTransactionReplayThreadPoolSize(1);
        conf.setNumHttpServerThreads(4);

        if (numBookies < 2) {
            conf.setManagedLedgerDefaultEnsembleSize(1);
            conf.setManagedLedgerDefaultWriteQuorum(1);
            conf.setManagedLedgerDefaultAckQuorum(1);
        }
        return conf;
    }
}
