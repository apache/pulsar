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

        if (metadataURL.startsWith(LocalMemoryMetadataStore.MEMORY_SCHEME_IDENTIFIER)) {
            return new LocalMemoryMetadataStore(metadataURL, metadataStoreConfig);
        } else if (metadataURL.startsWith(RocksdbMetadataStore.ROCKSDB_SCHEME_IDENTIFIER)) {
            return RocksdbMetadataStore.get(metadataURL, metadataStoreConfig);
        } else if (metadataURL.startsWith(EtcdMetadataStore.ETCD_SCHEME_IDENTIFIER)) {
            return new EtcdMetadataStore(metadataURL, metadataStoreConfig, enableSessionWatcher);
        } else if (metadataURL.startsWith(ZKMetadataStore.ZK_SCHEME_IDENTIFIER)) {
            return new ZKMetadataStore(metadataURL.substring(ZKMetadataStore.ZK_SCHEME_IDENTIFIER.length()),
                    metadataStoreConfig, enableSessionWatcher);
        } else {
            return new ZKMetadataStore(metadataURL, metadataStoreConfig, enableSessionWatcher);
        }
    }

    /**
     * Removes the identifier from the full metadata url.
     *
     * zk:my-zk:3000 -> my-zk:3000
     * etcd:my-etcd:3000 -> my-etcd:3000
     * my-default-zk:3000 -> my-default-zk:3000
     * @param metadataURL
     * @return
     */
    public static String removeIdentifierFromMetadataURL(String metadataURL) {
        if (metadataURL.startsWith(LocalMemoryMetadataStore.MEMORY_SCHEME_IDENTIFIER)) {
            return metadataURL.substring(LocalMemoryMetadataStore.MEMORY_SCHEME_IDENTIFIER.length());
        } else if (metadataURL.startsWith(RocksdbMetadataStore.ROCKSDB_SCHEME_IDENTIFIER)) {
            return metadataURL.substring(RocksdbMetadataStore.ROCKSDB_SCHEME_IDENTIFIER.length());
        } else if (metadataURL.startsWith(EtcdMetadataStore.ETCD_SCHEME_IDENTIFIER)) {
            return metadataURL.substring(EtcdMetadataStore.ETCD_SCHEME_IDENTIFIER.length());
        } else if (metadataURL.startsWith(ZKMetadataStore.ZK_SCHEME_IDENTIFIER)) {
            return metadataURL.substring(ZKMetadataStore.ZK_SCHEME_IDENTIFIER.length());
        }
        return metadataURL;
    }
}
