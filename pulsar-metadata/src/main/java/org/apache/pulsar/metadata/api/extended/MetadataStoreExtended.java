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
package org.apache.pulsar.metadata.api.extended;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.pulsar.metadata.api.MetadataEvent;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;

/**
 * Extension of the {@link MetadataStore} interface that includes more methods which might not be supported by all
 * implementations.
 */
public interface MetadataStoreExtended extends MetadataStore {

    static MetadataStoreExtended create(String metadataURL, MetadataStoreConfig metadataStoreConfig)
            throws MetadataStoreException {
        return MetadataStoreFactoryImpl.createExtended(metadataURL, metadataStoreConfig);
    }

    /**
     * Legacy {@code EnumSet<CreateOption>} form of {@link MetadataStore#put(String, byte[], Optional, Set)}.
     *
     * <p>Translates the {@link CreateOption} set into the canonical {@code Set<Option>} form and
     * forwards to {@link MetadataStore#put(String, byte[], Optional, Set)}.
     *
     * @param path
     *            the path of the key
     * @param value
     *            the value to store
     * @param expectedVersion
     *            if present, the version will have to match for the operation to succeed. Use -1 to enforce a
     *            non-existing value.
     * @param options
     *            a set of {@link CreateOption} to use if the key-value pair is being created
     * @throws BadVersionException
     *             if the expected version doesn't match the actual version of the data
     * @return a future to track the async request
     */
    default CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion,
            EnumSet<CreateOption> options) {
        return put(path, value, expectedVersion, toOptions(options, null));
    }

    /**
     * Legacy {@code EnumSet<CreateOption>} + {@code Map<String,String>} form of
     * {@link MetadataStore#put(String, byte[], Optional, Set)}.
     *
     * <p>Translates the inputs into the canonical {@code Set<Option>} form and forwards to
     * {@link MetadataStore#put(String, byte[], Optional, Set)}.
     *
     * @param path              the path of the key
     * @param value             the value to store
     * @param expectedVersion   if present, the version will have to match for the operation to succeed
     * @param options           a set of {@link CreateOption} to use if the key-value pair is being created
     * @param secondaryIndexes  secondary indexes to associate with this record (index name to secondary key)
     * @throws BadVersionException if the expected version doesn't match the actual version
     * @return a future to track the async request
     */
    default CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion,
            EnumSet<CreateOption> options, Map<String, String> secondaryIndexes) {
        return put(path, value, expectedVersion, toOptions(options, secondaryIndexes));
    }

    /**
     * Translate the legacy {@code EnumSet<CreateOption>} + {@code Map<String,String>} form into the
     * canonical {@code Set<Option>} form.
     */
    private static Set<Option> toOptions(EnumSet<CreateOption> options, Map<String, String> secondaryIndexes) {
        boolean hasOptions = options != null && !options.isEmpty();
        boolean hasIndexes = secondaryIndexes != null && !secondaryIndexes.isEmpty();
        if (!hasOptions && !hasIndexes) {
            return Set.of();
        }
        Set<Option> result = new HashSet<>();
        if (hasOptions) {
            if (options.contains(CreateOption.Ephemeral)) {
                result.add(Option.Ephemeral.INSTANCE);
            }
            if (options.contains(CreateOption.Sequential)) {
                result.add(Option.Sequential.INSTANCE);
            }
        }
        if (hasIndexes) {
            for (Map.Entry<String, String> e : secondaryIndexes.entrySet()) {
                result.add(new Option.SecondaryIndex(e.getKey(), e.getValue()));
            }
        }
        return result;
    }

    /**
     * Register a session listener that will get notified of changes in status of the current session.
     *
     * @param listener
     *            the session listener
     */
    void registerSessionListener(Consumer<SessionEvent> listener);

    /**
     * Get {@link MetadataEventSynchronizer} to notify and synchronize metadata events.
     *
     * @return
     */
    default Optional<MetadataEventSynchronizer> getMetadataEventSynchronizer() {
        return Optional.empty();
    }

    default void updateMetadataEventSynchronizer(MetadataEventSynchronizer synchronizer) {}

    /**
     * Handles a metadata synchronizer event.
     *
     * @param event
     * @return completed future when the event is handled
     */
    default CompletableFuture<Void> handleMetadataEvent(MetadataEvent event) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Force invalidation of cached entries for the specified paths.
     *
     * @param paths
     */
    default void invalidateCaches(String...paths) {
    }
}
