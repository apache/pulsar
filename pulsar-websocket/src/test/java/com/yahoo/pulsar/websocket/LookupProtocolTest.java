/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.websocket;

import java.lang.reflect.Field;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.pulsar.websocket.service.WebSocketProxyConfiguration;
import com.yahoo.pulsar.client.impl.PulsarClientImpl;

public class LookupProtocolTest {
    @Test
    public void httpLookupTest() throws Exception{
        WebSocketProxyConfiguration conf = new WebSocketProxyConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setServiceUrlTls("https://localhost:8443");
        WebSocketService service  = new WebSocketService(conf);
        PulsarClientImpl testClient = (PulsarClientImpl) service.getPulsarClient();
        Field lookupField = PulsarClientImpl.class.getDeclaredField("lookup");
        lookupField.setAccessible(true);
        Assert.assertEquals(lookupField.get(testClient).getClass().getName(), "com.yahoo.pulsar.client.impl.HttpLookupService");
        Assert.assertFalse(testClient.getConfiguration().isUseTls());
        service.close();
    }

    @Test
    public void httpsLookupTest() throws Exception{
        WebSocketProxyConfiguration conf = new WebSocketProxyConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setServiceUrlTls("https://localhost:8443");
        conf.setBrokerServiceUrl("pulsar://localhost:6650");
        conf.setTlsEnabled(true);
        WebSocketService service  = new WebSocketService(conf);
        PulsarClientImpl testClient = (PulsarClientImpl) service.getPulsarClient();
        Field lookupField = PulsarClientImpl.class.getDeclaredField("lookup");
        lookupField.setAccessible(true);
        Assert.assertEquals(lookupField.get(testClient).getClass().getName(), "com.yahoo.pulsar.client.impl.HttpLookupService");
        Assert.assertTrue(testClient.getConfiguration().isUseTls());
        service.close();
    }

    @Test
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
        Assert.assertEquals(lookupField.get(testClient).getClass().getName(), "com.yahoo.pulsar.client.impl.BinaryProtoLookupService");
        Assert.assertFalse(testClient.getConfiguration().isUseTls());
        service.close();
    }

    @Test
    public void binaryTlsLookupTest() throws Exception{
        WebSocketProxyConfiguration conf = new WebSocketProxyConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setServiceUrlTls("https://localhost:8443");
        conf.setBrokerServiceUrl("pulsar://localhost:6650");
        conf.setBrokerServiceUrlTls("pulsar+ssl://localhost:6651");
        conf.setTlsEnabled(true);
        WebSocketService service  = new WebSocketService(conf);
        PulsarClientImpl testClient = (PulsarClientImpl) service.getPulsarClient();
        Field lookupField = PulsarClientImpl.class.getDeclaredField("lookup");
        lookupField.setAccessible(true);
        Assert.assertEquals(lookupField.get(testClient).getClass().getName(), "com.yahoo.pulsar.client.impl.BinaryProtoLookupService");
        Assert.assertTrue(testClient.getConfiguration().isUseTls());
        service.close();
    }
}
