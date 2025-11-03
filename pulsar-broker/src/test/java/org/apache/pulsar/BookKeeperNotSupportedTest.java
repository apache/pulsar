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
package org.apache.pulsar;

import io.opentelemetry.api.OpenTelemetry;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.delayed.BucketDelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage;
import org.apache.pulsar.broker.service.schema.SchemaStorageFactory;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.compaction.DisabledTopicCompactionService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.jspecify.annotations.NonNull;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test the start will fail fast if managed ledger does not use BookKeeper as the default storage layer while some
 * plugins depend on BookKeeper.
 */
@Test
public class BookKeeperNotSupportedTest {

    @Test
    public void testSchemaStorage() throws PulsarServerException {
        final var config = newConfig();
        try (final var pulsar = new PulsarService(config)) {
            Assert.assertThrows(PulsarServerException.BookKeeperNotSupportedException.class, pulsar::start);
        }
    }

    @Test
    public void testDelayedDeliveryTracker() throws PulsarServerException {
        final var config = newConfig();
        config.setSchemaRegistryStorageClassName(DummySchemaFactory.class.getName());
        config.setDelayedDeliveryTrackerFactoryClassName(BucketDelayedDeliveryTrackerFactory.class.getName());
        try (final var pulsar = new PulsarService(config)) {
            Assert.assertThrows(PulsarServerException.BookKeeperNotSupportedException.class, pulsar::start);
        }
    }

    @Test
    public void testStartSucceed() throws PulsarServerException {
        final var config = newConfig();
        config.setSchemaRegistryStorageClassName(DummySchemaFactory.class.getName());
        try (final var ignored = new PulsarService(config)) {
        }
    }

    private static ServiceConfiguration newConfig() {
        final var config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setMetadataStoreUrl("memory:local");
        config.setManagedLedgerStorageClassName(NoBkStorage.class.getName());
        config.setCompactionServiceFactoryClassName(DisabledTopicCompactionService.class.getName());
        config.setAdvertisedAddress("127.0.0.1"); // avoid being affected by the local proxy
        return config;
    }

    private static class NoBkStorage implements ManagedLedgerStorage {

        final ManagedLedgerStorageClass storage = new ManagedLedgerStorageClass() {
            @Override
            public String getName() {
                return "other-storage";
            }

            @Override
            public ManagedLedgerFactory getManagedLedgerFactory() {
                return Mockito.mock(ManagedLedgerFactoryImpl.class);
            }
        };

        @Override
        public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                               OpenTelemetry openTelemetry) {
        }

        @Override
        public Collection<ManagedLedgerStorageClass> getStorageClasses() {
            return List.of(storage);
        }

        @Override
        public Optional<ManagedLedgerStorageClass> getManagedLedgerStorageClass(String name) {
            return Optional.of(storage);
        }

        @Override
        public void close() {
        }
    }

    static class DummySchemaFactory implements SchemaStorageFactory {

        @Override
        public @NonNull SchemaStorage create(PulsarService pulsar) throws Exception {
            return Mockito.mock(BookkeeperSchemaStorage.class);
        }
    }
}
