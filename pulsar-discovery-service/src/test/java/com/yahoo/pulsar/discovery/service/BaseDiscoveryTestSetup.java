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
package com.yahoo.pulsar.discovery.service;

import static com.yahoo.pulsar.discovery.service.web.ZookeeperCacheLoader.LOADBALANCE_BROKERS_ROOT;
import static org.apache.bookkeeper.test.PortManager.nextFreePort;

import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.util.concurrent.MoreExecutors;
import com.yahoo.pulsar.discovery.service.server.ServiceConfig;
import com.yahoo.pulsar.zookeeper.ZooKeeperClientFactory;
import com.yahoo.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class BaseDiscoveryTestSetup {

    protected ServiceConfig config;
    protected DiscoveryService service;
    protected MockZooKeeper mockZookKeeper;
    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";
    
    protected void setup() throws Exception {
        config = new ServiceConfig();
        config.setServicePort(nextFreePort());
        config.setServicePortTls(nextFreePort());
        config.setBindOnLocalhost(true);
        
        config.setTlsEnabled(true);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        
        mockZookKeeper = createMockZooKeeper();
        service = spy(new DiscoveryService(config));
        doReturn(mockZooKeeperClientFactory).when(service).getZooKeeperClientFactory();
        service.start();
        
    }
    
    protected void cleanup() throws Exception {
        mockZookKeeper.shutdown();
        service.close();
    }
 
    protected MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.sameThreadExecutor());

        ZkUtils.createFullPathOptimistic(zk, LOADBALANCE_BROKERS_ROOT,
                "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        return zk;
    }
    
    protected ZooKeeperClientFactory mockZooKeeperClientFactory = new ZooKeeperClientFactory() {

        @Override
        public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                int zkSessionTimeoutMillis) {
            // Always return the same instance (so that we don't loose the mock ZK content on broker restart
            return CompletableFuture.completedFuture(mockZookKeeper);
        }
    };
    
}
