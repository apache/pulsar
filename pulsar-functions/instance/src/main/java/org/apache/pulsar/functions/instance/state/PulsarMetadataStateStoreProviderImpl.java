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
package org.apache.pulsar.functions.instance.state;

import java.util.Map;
import lombok.SneakyThrows;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;

public class PulsarMetadataStateStoreProviderImpl implements StateStoreProvider {

    private static final String METADATA_URL = "METADATA_URL";
    private static final String METADATA_STORE_INSTANCE = "METADATA_STORE_INSTANCE";

    private static final String METADATA_PREFIX = "METADATA_PREFIX";
    private static final String METADATA_DEFAULT_PREFIX = "/state-store";

    private MetadataStore store;
    private String prefix;
    private boolean shouldCloseStore;

    @Override
    public void init(Map<String, Object> config, Function.FunctionDetails functionDetails) throws Exception {

        prefix = (String) config.getOrDefault(METADATA_PREFIX, METADATA_DEFAULT_PREFIX);

        if (config.containsKey(METADATA_STORE_INSTANCE)) {
            store = (MetadataStore) config.get(METADATA_STORE_INSTANCE);
            shouldCloseStore = false;
        } else {
            String metadataUrl = (String) config.get(METADATA_URL);
            store = MetadataStoreFactory.create(metadataUrl, MetadataStoreConfig.builder().build());
            shouldCloseStore = true;
        }
    }

    @Override
    public DefaultStateStore getStateStore(String tenant, String namespace, String name) throws Exception {
        return new PulsarMetadataStateStoreImpl(store, prefix, tenant, namespace, name);
    }

    @SneakyThrows
    @Override
    public void close() {
        if (shouldCloseStore) {
            store.close();
        }
    }
}
