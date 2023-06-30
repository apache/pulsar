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
package org.apache.pulsar.metadata.impl;

import static org.apache.pulsar.metadata.impl.EtcdMetadataStore.ETCD_SCHEME_IDENTIFIER;
import static org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore.MEMORY_SCHEME_IDENTIFIER;
import static org.apache.pulsar.metadata.impl.RocksdbMetadataStore.ROCKSDB_SCHEME_IDENTIFIER;
import static org.apache.pulsar.metadata.impl.ZKMetadataStore.ZK_SCHEME_IDENTIFIER;
import com.google.common.base.Splitter;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class MetadataStoreFactoryImpl {

    public static final String METADATASTORE_PROVIDERS_PROPERTY = "pulsar.metadatastore.providers";

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
        MetadataStoreProvider provider = findProvider(metadataURL);
        return provider.create(metadataURL, metadataStoreConfig, enableSessionWatcher);
    }

    static Map<String, MetadataStoreProvider> loadProviders() {
        Map<String, MetadataStoreProvider> providers = new HashMap<>();
        providers.put(MEMORY_SCHEME_IDENTIFIER, new MemoryMetadataStoreProvider());
        providers.put(ROCKSDB_SCHEME_IDENTIFIER, new RocksdbMetadataStoreProvider());
        providers.put(ETCD_SCHEME_IDENTIFIER, new EtcdMetadataStoreProvider());
        providers.put(ZK_SCHEME_IDENTIFIER, new ZkMetadataStoreProvider());

        String factoryClasses = System.getProperty(METADATASTORE_PROVIDERS_PROPERTY, "");

        for (String className : Splitter.on(',').trimResults().omitEmptyStrings().split(factoryClasses)) {
            try {
                Class<? extends MetadataStoreProvider> clazz =
                        (Class<? extends MetadataStoreProvider>) Class.forName(className);
                MetadataStoreProvider provider = clazz.getConstructor().newInstance();
                String scheme = provider.urlScheme();
                providers.put(scheme + ":", provider);
            } catch (Exception e) {
                log.warn("Failed to load metadata store provider class for name '{}'", className, e);
            }
        }
        return providers;
    }

    private static MetadataStoreProvider findProvider(String metadataURL) {
        Map<String, MetadataStoreProvider> providers = loadProviders();
        for (Map.Entry<String, MetadataStoreProvider> entry : providers.entrySet()) {
            if (metadataURL.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        return providers.get(ZK_SCHEME_IDENTIFIER);
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
        MetadataStoreProvider provider = findProvider(metadataURL);
        if (metadataURL.startsWith(provider.urlScheme() + ":")) {
            return metadataURL.substring(provider.urlScheme().length() + 1);
        }
        return metadataURL;
    }

    public static boolean isBasedOnZookeeper(String metadataURL) {
        if (!metadataURL.contains("://")) {
            return true;
        }

        return metadataURL.startsWith("zk");
    }
}
