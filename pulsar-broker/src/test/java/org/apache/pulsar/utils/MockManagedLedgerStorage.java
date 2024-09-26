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
package org.apache.pulsar.utils;

import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.Mockito;

public class MockManagedLedgerStorage implements ManagedLedgerStorage {

    private final MockManagedLedgerFactory factory = new MockManagedLedgerFactory();

    @Override
    public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                           BookKeeperClientFactory bookkeeperProvider, EventLoopGroup eventLoopGroup,
                           OpenTelemetry openTelemetry) throws Exception {
        factory.setStore(metadataStore);
    }

    @Override
    public ManagedLedgerFactory getManagedLedgerFactory() {
        return factory;
    }

    @Override
    public StatsProvider getStatsProvider() {
        return new NullStatsProvider();
    }

    @Override
    public BookKeeper getBookKeeperClient() {
        return Mockito.mock(BookKeeper.class);
    }

    @Override
    public void close() throws IOException {
    }
}
