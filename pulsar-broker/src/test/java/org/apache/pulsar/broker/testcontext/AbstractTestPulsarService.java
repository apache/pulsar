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

import java.io.IOException;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.service.PulsarMetadataEventSynchronizer;
import org.apache.pulsar.compaction.CompactionServiceFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * This is an internal class used by {@link PulsarTestContext} as the abstract base class for
 * {@link PulsarService} implementations for a PulsarService instance used in tests.
 * Please see {@link PulsarTestContext} for more details.
 */
abstract class AbstractTestPulsarService extends PulsarService {
    protected final SpyConfig spyConfig;

    public AbstractTestPulsarService(SpyConfig spyConfig, ServiceConfiguration config,
                                     MetadataStoreExtended localMetadataStore,
                                     MetadataStoreExtended configurationMetadataStore,
                                     CompactionServiceFactory compactionServiceFactory,
                                     BrokerInterceptor brokerInterceptor,
                                     BookKeeperClientFactory bookKeeperClientFactory) {
        super(config);
        this.spyConfig = spyConfig;
        setLocalMetadataStore(
                NonClosingProxyHandler.createNonClosingProxy(localMetadataStore, MetadataStoreExtended.class));
        setConfigurationMetadataStore(
                NonClosingProxyHandler.createNonClosingProxy(configurationMetadataStore, MetadataStoreExtended.class));
        super.setCompactionServiceFactory(compactionServiceFactory);
        setBrokerInterceptor(brokerInterceptor);
        setBkClientFactory(bookKeeperClientFactory);
    }

    @Override
    public MetadataStore createConfigurationMetadataStore(PulsarMetadataEventSynchronizer synchronizer)
            throws MetadataStoreException {
        if (synchronizer != null) {
            synchronizer.registerSyncListener(
                    ((MetadataStoreExtended) getConfigurationMetadataStore())::handleMetadataEvent);
        }
        return getConfigurationMetadataStore();
    }

    @Override
    public MetadataStoreExtended createLocalMetadataStore(PulsarMetadataEventSynchronizer synchronizer)
            throws MetadataStoreException, PulsarServerException {
        if (synchronizer != null) {
            synchronizer.registerSyncListener(
                    getLocalMetadataStore()::handleMetadataEvent);
        }
        return getLocalMetadataStore();
    }

    @Override
    public BookKeeperClientFactory newBookKeeperClientFactory() {
        return getBkClientFactory();
    }

    @Override
    protected BrokerInterceptor newBrokerInterceptor() throws IOException {
        return getBrokerInterceptor() != null ? getBrokerInterceptor() : super.newBrokerInterceptor();
    }
}
