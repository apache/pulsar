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
package org.apache.pulsar.websocket;

import java.lang.reflect.Field;

import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.netty.channel.epoll.Epoll;

public class LookupProtocolTest {

    static {
        // Trigger the loading of Epoll immediately, to avoid race condition in test
        Epoll.isAvailable();
    }

    @Test(timeOut = 10000)
    public void httpLookupTest() throws Exception{
        WebSocketProxyConfiguration conf = new WebSocketProxyConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setServiceUrlTls("https://localhost:8443");
        WebSocketService service  = new WebSocketService(conf);
        PulsarClientImpl testClient = (PulsarClientImpl) service.getPulsarClient();
        Field lookupField = PulsarClientImpl.class.getDeclaredField("lookup");
        lookupField.setAccessible(true);
        Assert.assertEquals(lookupField.get(testClient).getClass().getName(), "org.apache.pulsar.client.impl.HttpLookupService");
        service.close();
    }

    @Test(timeOut = 10000)
    public void httpsLookupTest() throws Exception{
        WebSocketProxyConfiguration conf = new WebSocketProxyConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setServiceUrlTls("https://localhost:8443");
        conf.setBrokerServiceUrl("pulsar://localhost:6650");
        conf.setBrokerClientTlsEnabled(true);
        WebSocketService service  = new WebSocketService(conf);
        PulsarClientImpl testClient = (PulsarClientImpl) service.getPulsarClient();
        Field lookupField = PulsarClientImpl.class.getDeclaredField("lookup");
        lookupField.setAccessible(true);
        Assert.assertEquals(lookupField.get(testClient).getClass().getName(), "org.apache.pulsar.client.impl.HttpLookupService");
        Assert.assertTrue(testClient.getConfiguration().isUseTls());
        service.close();
    }

    @Test(timeOut = 10000)
    public void binaryLookupTest() throws Exception{
        WebSocketProxyConfiguration conf = new WebSocketProxyConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setServiceUrlTls("https://localhost:8443");
        conf.setBrokerServiceUrl("pulsar://localhost:6650");
        conf.setBrokerServiceUrlTls("pulsar+ssl://localhost:6651");
        WebSocketService service  = new WebSocketService(conf);
        PulsarClientImpl testClient = (PulsarClientImpl) service.getPulsarClient();
        Field lookupField = PulsarClientImpl.class.getDeclaredField("lookup");
        lookupField.setAccessible(true);
        Assert.assertEquals(lookupField.get(testClient).getClass().getName(), "org.apache.pulsar.client.impl.BinaryProtoLookupService");
        service.close();
    }

    @Test(timeOut = 10000)
    public void binaryTlsLookupTest() throws Exception{
        WebSocketProxyConfiguration conf = new WebSocketProxyConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setServiceUrlTls("https://localhost:8443");
        conf.setBrokerServiceUrl("pulsar://localhost:6650");
        conf.setBrokerServiceUrlTls("pulsar+ssl://localhost:6651");
        conf.setBrokerClientTlsEnabled(true);
        WebSocketService service  = new WebSocketService(conf);
        PulsarClientImpl testClient = (PulsarClientImpl) service.getPulsarClient();
        Field lookupField = PulsarClientImpl.class.getDeclaredField("lookup");
        lookupField.setAccessible(true);
        Assert.assertEquals(lookupField.get(testClient).getClass().getName(), "org.apache.pulsar.client.impl.BinaryProtoLookupService");
        Assert.assertTrue(testClient.getConfiguration().isUseTls());
        service.close();
    }
}
