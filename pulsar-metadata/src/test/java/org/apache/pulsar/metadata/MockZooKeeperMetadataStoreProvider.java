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
package org.apache.pulsar.metadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.MockZooKeeperSession;

public class MockZooKeeperMetadataStoreProvider implements MetadataStoreProvider {
    private static final String MOCK_ZK_SCHEME = "mock-zk";
    private static final ConcurrentMap<String, MockZooKeeper> mockZooKeepers = new ConcurrentHashMap<>();

    @Override
    public String urlScheme() {
        return MOCK_ZK_SCHEME;
    }

    @Override
    public MetadataStore create(String metadataURL, MetadataStoreConfig metadataStoreConfig,
                                boolean enableSessionWatcher) throws MetadataStoreException {
        MockZooKeeper mockZooKeeper = mockZooKeepers.computeIfAbsent(metadataURL,
                k -> MockZooKeeper.newInstance().registerCloseable(() -> mockZooKeepers.remove(k)));
        MockZooKeeperSession mockZooKeeperSession = MockZooKeeperSession.newInstance(mockZooKeeper, true);
        ZKMetadataStore zkMetadataStore = new ZKMetadataStore(mockZooKeeperSession, metadataStoreConfig, true);
        return zkMetadataStore;
    }
}
