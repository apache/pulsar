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
package org.apache.pulsar.metadata.impl.oxia;

import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;

public class OxiaMetadataStoreProvider implements MetadataStoreProvider {
    // declare the specific namespace to avoid any changes in the future.
    public static final String DefaultNamespace = "default";

    public static final  String OXIA_SCHEME = "oxia";
    public static final String OXIA_SCHEME_IDENTIFIER = OXIA_SCHEME + ":";

    @Override
    public String urlScheme() {
        return OXIA_SCHEME;
    }

    @Override
    public @NonNull MetadataStore create(
            String metadataURL, MetadataStoreConfig metadataStoreConfig, boolean enableSessionWatcher)
            throws MetadataStoreException {
        var serviceAddress = getServiceAddressAndNamespace(metadataURL);
        try {
            return new OxiaMetadataStore(
                    serviceAddress.getLeft(),
                    serviceAddress.getRight(),
                    metadataStoreConfig,
                    enableSessionWatcher);
        } catch (Exception e) {
            throw new MetadataStoreException(e);
        }
    }

    @NonNull
    Pair<String, String> getServiceAddressAndNamespace(String metadataURL)
            throws MetadataStoreException {
        if (metadataURL == null || !metadataURL.startsWith(urlScheme() + "://")) {
            throw new MetadataStoreException("Invalid metadata URL. Must start with 'oxia://'.");
        }
        final var addressWithNamespace = metadataURL.substring("oxia://".length());
        final var split = addressWithNamespace.split("/");
        if (split.length > 2) {
            throw new MetadataStoreException(
                    "Invalid metadata URL."
                            + " the oxia metadata format should be 'oxia://host:port/[namespace]'.");
        }
        if (split.length == 1) {
            // Use default namespace
            return Pair.of(split[0], DefaultNamespace);
        }
        return Pair.of(split[0], split[1]);
    }
}
