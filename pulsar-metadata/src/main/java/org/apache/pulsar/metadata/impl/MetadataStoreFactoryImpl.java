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

import static org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore.MEMORY_SCHEME_IDENTIFIER;
import static org.apache.pulsar.metadata.impl.RocksdbMetadataStore.ROCKSDB_SCHEME_IDENTIFIER;
import static org.apache.pulsar.metadata.impl.ZKMetadataStore.ZK_SCHEME_IDENTIFIER;
import static org.apache.pulsar.metadata.impl.oxia.OxiaMetadataStoreProvider.OXIA_SCHEME_IDENTIFIER;
import com.google.common.base.Splitter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.oxia.OxiaMetadataStoreProvider;

@Slf4j
public class MetadataStoreFactoryImpl {

    public static final String METADATASTORE_PROVIDERS_PROPERTY = "pulsar.metadatastore.providers";
    static final String CONFIG_FILE_PATH_METADATA_URL_PARAM = "configFilePath";

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
        String metadataURLWithoutConfigFilePath = removeMetadataURLQueryParam(
                metadataURL, CONFIG_FILE_PATH_METADATA_URL_PARAM);
        MetadataStoreProvider provider = findProvider(metadataURLWithoutConfigFilePath);
        MetadataStoreConfig effectiveConfig = applyMetadataURLQueryParams(metadataURL, metadataStoreConfig);
        return provider.create(metadataURLWithoutConfigFilePath, effectiveConfig, enableSessionWatcher);
    }

    @SuppressWarnings("auxiliaryclass")
    static Map<String, MetadataStoreProvider> loadProviders() {
        Map<String, MetadataStoreProvider> providers = new HashMap<>();
        providers.put(MEMORY_SCHEME_IDENTIFIER, new MemoryMetadataStoreProvider());
        providers.put(ROCKSDB_SCHEME_IDENTIFIER, new RocksdbMetadataStoreProvider());
        providers.put(OXIA_SCHEME_IDENTIFIER, new OxiaMetadataStoreProvider());
        providers.put(ZK_SCHEME_IDENTIFIER, new ZkMetadataStoreProvider());

        String factoryClasses = System.getProperty(METADATASTORE_PROVIDERS_PROPERTY, "");

        for (String className : Splitter.on(',').trimResults().omitEmptyStrings().split(factoryClasses)) {
            try {
                @SuppressWarnings("unchecked")
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
        if (metadataURL.startsWith("etcd:")) {
            throw new IllegalArgumentException(
                    "Etcd metadata store backend has been removed in Pulsar 5.0 (PIP-462). "
                            + "Please use ZooKeeper (zk:) or Oxia (oxia:) as your metadata store.");
        }
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
     * my-default-zk:3000 -> my-default-zk:3000
     * @param metadataURL
     * @return
     */
    public static String removeIdentifierFromMetadataURL(String metadataURL) {
        String metadataURLWithoutConfigFilePath = removeMetadataURLQueryParam(
                metadataURL, CONFIG_FILE_PATH_METADATA_URL_PARAM);
        MetadataStoreProvider provider = findProvider(metadataURLWithoutConfigFilePath);
        if (metadataURLWithoutConfigFilePath.startsWith(provider.urlScheme() + ":")) {
            return metadataURLWithoutConfigFilePath.substring(provider.urlScheme().length() + 1);
        }
        return metadataURLWithoutConfigFilePath;
    }

    public static boolean isBasedOnZookeeper(String metadataURL) {
        if (!metadataURL.contains("://")) {
            return true;
        }

        return metadataURL.startsWith("zk");
    }

    static String removeMetadataURLQueryParam(String metadataURL, String paramName) {
        int queryStart = metadataURL.indexOf('?');
        if (queryStart < 0) {
            return metadataURL;
        }

        int fragmentStart = metadataURL.indexOf('#', queryStart);
        String query = metadataURL.substring(queryStart + 1,
                fragmentStart >= 0 ? fragmentStart : metadataURL.length());
        StringBuilder queryBuilder = new StringBuilder();
        boolean removed = false;
        for (String param : Splitter.on('&').omitEmptyStrings().split(query)) {
            int separator = param.indexOf('=');
            String key = separator >= 0 ? param.substring(0, separator) : param;
            if (isMetadataURLQueryParam(key, paramName)) {
                removed = true;
                continue;
            }
            if (queryBuilder.length() > 0) {
                queryBuilder.append('&');
            }
            queryBuilder.append(param);
        }

        if (!removed) {
            return metadataURL;
        }

        String metadataURLWithoutQuery = metadataURL.substring(0, queryStart);
        String fragment = fragmentStart >= 0 ? metadataURL.substring(fragmentStart) : "";
        if (queryBuilder.length() == 0) {
            return metadataURLWithoutQuery + fragment;
        }
        return metadataURLWithoutQuery + '?' + queryBuilder + fragment;
    }

    private static boolean isMetadataURLQueryParam(String key, String paramName) {
        try {
            return paramName.equals(URLDecoder.decode(key, StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    static MetadataStoreConfig applyMetadataURLQueryParams(
            String metadataURL, MetadataStoreConfig metadataStoreConfig) throws MetadataStoreException {
        Map<String, String> params = parseMetadataURLQuery(metadataURL);
        String configFilePath = params.get(CONFIG_FILE_PATH_METADATA_URL_PARAM);
        String existingConfigFilePath = metadataStoreConfig.getConfigFilePath();
        if (configFilePath == null || (existingConfigFilePath != null && !existingConfigFilePath.isEmpty())) {
            return metadataStoreConfig;
        }

        return MetadataStoreConfig.builder()
                .sessionTimeoutMillis(metadataStoreConfig.getSessionTimeoutMillis())
                .allowReadOnlyOperations(metadataStoreConfig.isAllowReadOnlyOperations())
                .configFilePath(configFilePath)
                .batchingEnabled(metadataStoreConfig.isBatchingEnabled())
                .batchingMaxDelayMillis(metadataStoreConfig.getBatchingMaxDelayMillis())
                .batchingMaxOperations(metadataStoreConfig.getBatchingMaxOperations())
                .batchingMaxSizeKb(metadataStoreConfig.getBatchingMaxSizeKb())
                .metadataStoreName(metadataStoreConfig.getMetadataStoreName())
                .fsyncEnable(metadataStoreConfig.isFsyncEnable())
                .synchronizer(metadataStoreConfig.getSynchronizer())
                .openTelemetry(metadataStoreConfig.getOpenTelemetry())
                .nodeSizeStats(metadataStoreConfig.getNodeSizeStats())
                .numSerDesThreads(metadataStoreConfig.getNumSerDesThreads())
                .build();
    }

    private static Map<String, String> parseMetadataURLQuery(String metadataURL) throws MetadataStoreException {
        int queryStart = metadataURL.indexOf('?');
        if (queryStart < 0 || queryStart == metadataURL.length() - 1) {
            return Map.of();
        }

        int fragmentStart = metadataURL.indexOf('#', queryStart);
        String query = metadataURL.substring(queryStart + 1,
                fragmentStart >= 0 ? fragmentStart : metadataURL.length());
        Map<String, String> params = new HashMap<>();
        for (String param : Splitter.on('&').omitEmptyStrings().split(query)) {
            int separator = param.indexOf('=');
            String key = separator >= 0 ? param.substring(0, separator) : param;
            String value = separator >= 0 ? param.substring(separator + 1) : "";
            params.put(decodeMetadataURLQueryParam(key), decodeMetadataURLQueryParam(value));
        }
        return params;
    }

    private static String decodeMetadataURLQueryParam(String value) throws MetadataStoreException {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new MetadataStoreException("Invalid metadata URL query parameter", e);
        }
    }
}
