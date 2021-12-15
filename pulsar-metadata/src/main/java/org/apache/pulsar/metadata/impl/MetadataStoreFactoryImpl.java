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
package org.apache.pulsar.metadata.impl;

import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

public class MetadataStoreFactoryImpl {

    public static MetadataStore create(String metadataURL, MetadataStoreConfig metadataStoreConfig) throws
            MetadataStoreException {
        return newInstance(metadataURL, metadataStoreConfig, false);
    }

    public static MetadataStoreExtended createExtended(String metadataURL, MetadataStoreConfig metadataStoreConfig)
            throws
            MetadataStoreException {
        MetadataStore store = MetadataStoreFactoryImpl.newInstance(metadataURL, metadataStoreConfig, true);
        if (!(store instanceof MetadataStoreExtended)) {
            throw new MetadataStoreException.InvalidImplementationException(
                    "Implementation does not comply with " + MetadataStoreExtended.class.getName());
        }

        return (MetadataStoreExtended) store;
    }

    private static MetadataStore newInstance(String metadataURL, MetadataStoreConfig metadataStoreConfig,
                                             boolean enableSessionWatcher)
            throws MetadataStoreException {

        if (metadataURL.startsWith("memory://")) {
            return new LocalMemoryMetadataStore(metadataURL, metadataStoreConfig);
        }
        if (metadataURL.startsWith("rocksdb://")) {
            return RocksdbMetadataStore.get(metadataURL, metadataStoreConfig);
        } else {
            return new ZKMetadataStore(metadataURL, metadataStoreConfig, enableSessionWatcher);
        }
    }
}
