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
package org.apache.pulsar.websocket.proxy;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import java.util.Optional;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.websocket.WebSocketService;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "websocket")
public class ProxyConfigurationTest extends ProducerConsumerBase {
    private WebSocketProxyConfiguration config;

    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        config = new WebSocketProxyConfiguration();
        config.setWebServicePort(Optional.of(0));
        config.setClusterName("test");
        config.setConfigurationMetadataStoreUrl(GLOBAL_DUMMY_VALUE);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "setProxyConfig")
    public Object[][] setProxyConfig() {
        return new Object[][] { {2, 1}, {4, 2} };
    }

    @Test(dataProvider = "setProxyConfig", timeOut = 10000)
    public void configTest(int numIoThreads, int connectionsPerBroker) throws Exception {
        config.setWebSocketNumIoThreads(numIoThreads);
        config.setWebSocketConnectionsPerBroker(connectionsPerBroker);
        config.getProperties().setProperty("brokerClient_serviceUrl", "https://broker.com:8080");
        config.setServiceUrl("http://localhost:8080");
        config.getProperties().setProperty("brokerClient_lookupTimeoutMs", "100");
        WebSocketService service = spyWithClassAndConstructorArgs(WebSocketService.class, config);
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(service)
                .createConfigMetadataStore(anyString(), anyInt(), anyBoolean());
        service.start();

        PulsarClientImpl client = (PulsarClientImpl) service.getPulsarClient();
        assertEquals(client.getConfiguration().getNumIoThreads(), numIoThreads);
        assertEquals(client.getConfiguration().getConnectionsPerBroker(), connectionsPerBroker);
        assertEquals(client.getConfiguration().getServiceUrl(), "http://localhost:8080",
                "brokerClient_ configs take precedence");
        assertEquals(client.getConfiguration().getLookupTimeoutMs(), 100);

        service.close();
    }
}
