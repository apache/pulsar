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
package org.apache.pulsar.broker.storage;

import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.classification.InterfaceAudience.Private;
import org.apache.pulsar.common.classification.InterfaceStability.Unstable;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * Storage to access {@link org.apache.bookkeeper.mledger.ManagedLedger}s.
 * <p>
 * The interface provides the abstraction to access the storage layer for managed ledgers.
 * The interface supports multiple storage classes, each with its own configuration. The default
 * implementation supports a single instance of {@link BookkeeperManagedLedgerStorageClass}.
 * Implementations can provide multiple storage classes. The default storage class is used
 * for topics unless it is overridden by the persistency policy at topic or namespace level.
 */
@Private
@Unstable
public interface ManagedLedgerStorage extends AutoCloseable {

    /**
     * Initialize the managed ledger storage.
     *
     * @param conf service config
     * @param bookkeeperProvider bookkeeper provider
     * @throws Exception
     */
    void initialize(ServiceConfiguration conf,
                    MetadataStoreExtended metadataStore,
                    BookKeeperClientFactory bookkeeperProvider,
                    EventLoopGroup eventLoopGroup,
                    OpenTelemetry openTelemetry) throws Exception;

    /**
     * Get all configured storage class instances.
     * @return all configured storage class instances
     */
    Collection<ManagedLedgerStorageClass> getStorageClasses();

    /**
     * Get the default storage class.
     * @return default storage class
     */
    default ManagedLedgerStorageClass getDefaultStorageClass() {
        return getStorageClasses().stream().findFirst().get();
    }

    /**
     * Lookup a storage class by name.
     * @param name storage class name
     * @return storage class instance, or empty if not found
     */
    Optional<ManagedLedgerStorageClass> getManagedLedgerStorageClass(String name);

    /**
     * Close the storage.
     *
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * Initialize the {@link ManagedLedgerStorage} from the provided resources.
     *
     * @param conf service config
     * @param bkProvider bookkeeper client provider
     * @return the initialized managed ledger storage.
     */
    static ManagedLedgerStorage create(ServiceConfiguration conf,
                                       MetadataStoreExtended metadataStore,
                                       BookKeeperClientFactory bkProvider,
                                       EventLoopGroup eventLoopGroup,
                                       OpenTelemetry openTelemetry) throws Exception {
        ManagedLedgerStorage storage =
                Reflections.createInstance(conf.getManagedLedgerStorageClassName(), ManagedLedgerStorage.class,
                        Thread.currentThread().getContextClassLoader());
        storage.initialize(conf, metadataStore, bkProvider, eventLoopGroup, openTelemetry);
        return storage;
    }
}
