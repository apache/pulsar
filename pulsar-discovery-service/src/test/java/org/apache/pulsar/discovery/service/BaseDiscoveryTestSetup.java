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
package org.apache.pulsar.discovery.service;

import static org.apache.pulsar.broker.resources.MetadataStoreCacheLoader.LOADBALANCE_BROKERS_ROOT;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.util.concurrent.MoreExecutors;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.pulsar.discovery.service.server.ServiceConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.KeeperException.Code;


public class BaseDiscoveryTestSetup {

    protected ServiceConfig config;
    protected DiscoveryService service;
    private MockZooKeeper mockZooKeeper;
    protected MetadataStoreExtended zkStore;
    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";

    protected void setup() throws Exception {
        config = new ServiceConfig();
        config.setServicePort(Optional.of(0));
        config.setServicePortTls(Optional.of(0));
        config.setBindOnLocalhost(true);

        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);

        mockZooKeeper = createMockZooKeeper();
        zkStore = new ZKMetadataStore(mockZooKeeper);
        zkStore.put(LOADBALANCE_BROKERS_ROOT, "".getBytes(StandardCharsets.UTF_8),
                Optional.of(-1L));
        service = spy(new DiscoveryService(config));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(service).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(service).createConfigurationMetadataStore();
        service.start();

    }

    protected void cleanup() throws Exception {
        mockZooKeeper.shutdown();
        zkStore.close();
        service.close();
    }

    protected MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        return zk;
    }

    protected void simulateStoreError(String string, Code sessionexpired) {
        mockZooKeeper.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.GET
                && path.equals("/admin/partitioned-topics/test/local/ns/persistent/my-topic-2");
        });
    }

    protected void simulateStoreErrorForNonPersistentTopic(String string, Code sessionexpired) {
        mockZooKeeper.failConditional(Code.SESSIONEXPIRED, (op, path) -> {
            return op == MockZooKeeper.Op.GET
                    && path.equals("/admin/partitioned-topics/test/local/ns/non-persistent/my-topic-2");
        });
    }
}
