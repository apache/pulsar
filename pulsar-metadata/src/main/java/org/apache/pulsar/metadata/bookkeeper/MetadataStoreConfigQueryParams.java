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
package org.apache.pulsar.metadata.bookkeeper;

import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreConfig.MetadataStoreConfigBuilder;

class MetadataStoreConfigQueryParams {
    private static final Map<String, BiConsumer<MetadataStoreConfigBuilder, String>> PARAM_SETTERS = Map.ofEntries(
            booleanParam("allowReadOnlyOperations", MetadataStoreConfigBuilder::allowReadOnlyOperations),
            booleanParam("batchingEnabled", MetadataStoreConfigBuilder::batchingEnabled),
            nonNegativeIntParam("batchingMaxDelayMillis", MetadataStoreConfigBuilder::batchingMaxDelayMillis),
            positiveIntParam("batchingMaxOperations", MetadataStoreConfigBuilder::batchingMaxOperations),
            positiveIntParam("batchingMaxSizeKb", MetadataStoreConfigBuilder::batchingMaxSizeKb),
            Map.entry("configFilePath", MetadataStoreConfigBuilder::configFilePath),
            booleanParam("fsyncEnable", MetadataStoreConfigBuilder::fsyncEnable),
            positiveIntParam("numSerDesThreads", MetadataStoreConfigBuilder::numSerDesThreads),
            positiveIntParam("sessionTimeoutMillis", MetadataStoreConfigBuilder::sessionTimeoutMillis));

    private MetadataStoreConfigQueryParams() {
    }

    static boolean contains(String key) {
        return PARAM_SETTERS.containsKey(key);
    }

    @SuppressWarnings("rawtypes")
    static MetadataStoreConfig createConfig(AbstractConfiguration conf, Map<String, String> configParams) {
        MetadataStoreConfigBuilder builder = MetadataStoreConfig.builder()
                .sessionTimeoutMillis(conf.getZkTimeout())
                .metadataStoreName(MetadataStoreConfig.METADATA_STORE);
        configParams.forEach((key, value) -> {
            BiConsumer<MetadataStoreConfigBuilder, String> setter = PARAM_SETTERS.get(key);
            if (setter == null) {
                throw new IllegalArgumentException("Unsupported MetadataStoreConfig query parameter '" + key
                        + "'. Supported parameters are " + PARAM_SETTERS.keySet());
            }
            setter.accept(builder, value);
        });
        return builder.build();
    }

    private static Map.Entry<String, BiConsumer<MetadataStoreConfigBuilder, String>> booleanParam(
            String key, BiConsumer<MetadataStoreConfigBuilder, Boolean> setter) {
        return Map.entry(key, (builder, value) -> setter.accept(builder, parseBoolean(key, value)));
    }

    private static Map.Entry<String, BiConsumer<MetadataStoreConfigBuilder, String>> nonNegativeIntParam(
            String key, BiConsumer<MetadataStoreConfigBuilder, Integer> setter) {
        return Map.entry(key, (builder, value) -> setter.accept(builder, parseNonNegativeInt(key, value)));
    }

    private static Map.Entry<String, BiConsumer<MetadataStoreConfigBuilder, String>> positiveIntParam(
            String key, BiConsumer<MetadataStoreConfigBuilder, Integer> setter) {
        return Map.entry(key, (builder, value) -> setter.accept(builder, parsePositiveInt(key, value)));
    }

    private static boolean parseBoolean(String key, String value) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        } else if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("MetadataStoreConfig query parameter '" + key
                + "' must be true or false");
    }

    private static int parseNonNegativeInt(String key, String value) {
        int intValue = parseInt(key, value);
        if (intValue < 0) {
            throw new IllegalArgumentException("MetadataStoreConfig query parameter '" + key
                    + "' must be greater than or equal to 0");
        }
        return intValue;
    }

    private static int parsePositiveInt(String key, String value) {
        int intValue = parseInt(key, value);
        if (intValue <= 0) {
            throw new IllegalArgumentException("MetadataStoreConfig query parameter '" + key
                    + "' must be greater than 0");
        }
        return intValue;
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("MetadataStoreConfig query parameter '" + key
                    + "' must be an integer", e);
        }
    }
}
