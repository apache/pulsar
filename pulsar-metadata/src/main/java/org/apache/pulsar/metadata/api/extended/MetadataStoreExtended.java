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
package org.apache.pulsar.metadata.api.extended;

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.InvalidImplementationException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Stat;

/**
 * Extension of the {@link MetadataStore} interface that includes more methods which might not be supported by all
 * implementations.
 */
public interface MetadataStoreExtended extends MetadataStore {

    public static MetadataStoreExtended create(String metadataURL, MetadataStoreConfig metadataStoreConfig)
            throws MetadataStoreException {
        MetadataStore store = MetadataStoreFactory.create(metadataURL, metadataStoreConfig);
        if (!(store instanceof MetadataStoreExtended)) {
            throw new InvalidImplementationException(
                    "Implemetation does not comply with " + MetadataStoreExtended.class.getName());
        }

        return (MetadataStoreExtended) store;
    }

    /**
     * Put a new value for a given key.
     *
     * The caller can specify an expected version to be atomically checked against the current version of the stored
     * data.
     *
     * The future will return the {@link Stat} object associated with the newly inserted value.
     *
     *
     * @param path
     *            the path of the key to delete from the store
     * @param value
     *            the value to
     * @param expectedVersion
     *            if present, the version will have to match with the currently stored value for the operation to
     *            succeed. Use -1 to enforce a non-existing value.
     * @param options
     *            a set of {@link CreateOption} to use if the the key-value pair is being created
     * @throws BadVersionException
     *             if the expected version doesn't match the actual version of the data
     * @return a future to track the async request
     */
    CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion,
            EnumSet<CreateOption> options);
}
