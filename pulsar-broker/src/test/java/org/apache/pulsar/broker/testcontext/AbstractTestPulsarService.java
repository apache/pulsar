/*
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

package org.apache.pulsar.broker.testcontext;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.PulsarMetadataEventSynchronizer;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

abstract class AbstractTestPulsarService extends PulsarService {
    protected final SpyConfig spyConfig;
    protected final MetadataStoreExtended localMetadataStore;
    protected final MetadataStoreExtended configurationMetadataStore;
    protected final Compactor compactor;
    protected final BrokerInterceptor brokerInterceptor;
    protected final BookKeeperClientFactory bookKeeperClientFactory;
    private final boolean useSameThreadOrderedExecutor;

    public AbstractTestPulsarService(SpyConfig spyConfig, ServiceConfiguration config,
                                     MetadataStoreExtended localMetadataStore,
                                     MetadataStoreExtended configurationMetadataStore, Compactor compactor,
                                     BrokerInterceptor brokerInterceptor,
                                     BookKeeperClientFactory bookKeeperClientFactory,
                                     boolean useSameThreadOrderedExecutor) {
        super(config);
        this.spyConfig = spyConfig;
        this.localMetadataStore =
                NonClosingProxyHandler.createNonClosingProxy(localMetadataStore, MetadataStoreExtended.class);
        this.configurationMetadataStore =
                NonClosingProxyHandler.createNonClosingProxy(configurationMetadataStore, MetadataStoreExtended.class);
        this.compactor = compactor;
        this.brokerInterceptor = brokerInterceptor;
        this.bookKeeperClientFactory = bookKeeperClientFactory;
        this.useSameThreadOrderedExecutor = useSameThreadOrderedExecutor;
    }

    @Override
    public MetadataStore createConfigurationMetadataStore(PulsarMetadataEventSynchronizer synchronizer)
            throws MetadataStoreException {
        if (synchronizer != null) {
            synchronizer.registerSyncListener(configurationMetadataStore::handleMetadataEvent);
        }
        return configurationMetadataStore;
    }

    @Override
    public MetadataStoreExtended createLocalMetadataStore(PulsarMetadataEventSynchronizer synchronizer)
            throws MetadataStoreException, PulsarServerException {
        if (synchronizer != null) {
            synchronizer.registerSyncListener(localMetadataStore::handleMetadataEvent);
        }
        return localMetadataStore;
    }

    @Override
    public Compactor newCompactor() throws PulsarServerException {
        if (compactor != null) {
            return compactor;
        } else {
            return spyConfig.getCompactor().spy(super.newCompactor());
        }
    }

    @Override
    public BrokerInterceptor getBrokerInterceptor() {
        if (brokerInterceptor != null) {
            return brokerInterceptor;
        } else {
            return super.getBrokerInterceptor();
        }
    }

    @Override
    public BookKeeperClientFactory newBookKeeperClientFactory() {
        return bookKeeperClientFactory;
    }

    @Override
    protected OrderedExecutor newOrderedExecutor() {
        if (useSameThreadOrderedExecutor) {
            return new SameThreadOrderedSafeExecutor();
        } else {
            return super.newOrderedExecutor();
        }
    }
}
