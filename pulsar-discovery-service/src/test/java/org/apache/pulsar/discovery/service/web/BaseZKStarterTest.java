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
package org.apache.pulsar.discovery.service.web;

import static org.apache.pulsar.broker.resources.MetadataStoreCacheLoader.LOADBALANCE_BROKERS_ROOT;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.MockZooKeeper;

import com.google.common.util.concurrent.MoreExecutors;

public class BaseZKStarterTest {

    private MockZooKeeper mockZooKeeper;
    protected MetadataStoreExtended zkStore;

    protected void start() throws Exception {
        mockZooKeeper = createMockZooKeeper();
        zkStore = new ZKMetadataStore(mockZooKeeper);
        zkStore.put(LOADBALANCE_BROKERS_ROOT, "".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get();
    }

    protected void close() throws Exception {
        mockZooKeeper.shutdown();
        zkStore.close();
    }

    /**
     * Create MockZookeeper instance
     * @return
     * @throws Exception
     */
    protected MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        return zk;
    }

}
