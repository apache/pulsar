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

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * This is an internal class used by {@link PulsarTestContext} as the {@link PulsarService} implementation
 * for a "startable" PulsarService. Please see {@link PulsarTestContext} for more details.
 */
class StartableTestPulsarService extends AbstractTestPulsarService {
    private final Function<BrokerService, BrokerService> brokerServiceCustomizer;

    public StartableTestPulsarService(SpyConfig spyConfig, ServiceConfiguration config,
                                      MetadataStoreExtended localMetadataStore,
                                      MetadataStoreExtended configurationMetadataStore,
                                      Compactor compactor,
                                      BrokerInterceptor brokerInterceptor,
                                      BookKeeperClientFactory bookKeeperClientFactory,
                                      Function<BrokerService, BrokerService> brokerServiceCustomizer) {
        super(spyConfig, config, localMetadataStore, configurationMetadataStore, compactor, brokerInterceptor,
                bookKeeperClientFactory);
        this.brokerServiceCustomizer = brokerServiceCustomizer;
    }

    @Override
    protected BrokerService newBrokerService(PulsarService pulsar) throws Exception {
        return brokerServiceCustomizer.apply(super.newBrokerService(pulsar));
    }

    @Override
    public Supplier<NamespaceService> getNamespaceServiceProvider() throws PulsarServerException {
        return () -> spyConfig.getNamespaceService().spy(NamespaceService.class, this);
    }
}
