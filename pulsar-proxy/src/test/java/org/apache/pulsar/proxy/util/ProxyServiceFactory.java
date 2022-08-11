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
package org.apache.pulsar.proxy.util;

import static org.mockito.Mockito.doReturn;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.impl.AutoCloseUselessClientConSupports;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.Mockito;

@Slf4j
public class ProxyServiceFactory extends AutoCloseUselessClientConSupports {

    public static ProxyServiceInfo startProxyService(ZooKeeper localMetadataStore, ZooKeeper configurationMetadataStore,
                                                     Set<Integer> deniedPort) throws Exception{
        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setMetadataStoreUrl("DUMMY_VALUE");
        proxyConfig.setConfigurationMetadataStoreUrl("GLOBAL_DUMMY_VALUE");
        proxyConfig.setMaxConcurrentLookupRequests(100);
        proxyConfig.setMaxConcurrentInboundConnections(100);
        proxyConfig.setAuthenticationEnabled(false);
        proxyConfig.setBindAddress("127.0.0.1");
        int port = choosePort(deniedPort);
        log.info("Proxy service use port : {}", port);
        proxyConfig.setServicePort(Optional.of(port));
        ProxyService proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(new ZKMetadataStore(localMetadataStore)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(configurationMetadataStore)).when(proxyService)
                .createConfigurationMetadataStore();
        proxyService.start();
        InetSocketAddress proxyAddress =
                InetSocketAddress.createUnresolved(proxyConfig.getBindAddress(), proxyConfig.getServicePort().get());
        return new ProxyServiceInfo(proxyService, proxyConfig, proxyAddress);
    }

    public static int choosePort(Set<Integer> deniedPort){
        while (true){
            int randomPort = 10000 + new Random().nextInt(10000);
            if (deniedPort.contains(randomPort)){
                continue;
            }
            return randomPort;
        }
    }
}
