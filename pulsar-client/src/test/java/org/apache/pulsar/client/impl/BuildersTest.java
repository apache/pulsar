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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.testng.annotations.Test;

public class BuildersTest {

    @Test
    public void clientBuilderTest() {
        ClientBuilderImpl clientBuilder = (ClientBuilderImpl) PulsarClient.builder().ioThreads(10)
                .maxNumberOfRejectedRequestPerConnection(200).serviceUrl("pulsar://service:6650");

        assertEquals(clientBuilder.conf.isUseTls(), false);
        assertEquals(clientBuilder.conf.getServiceUrl(), "pulsar://service:6650");

        ClientBuilderImpl b2 = (ClientBuilderImpl) clientBuilder.clone();
        assertTrue(b2 != clientBuilder);

        b2.serviceUrl("pulsar://other-broker:6650");

        assertEquals(clientBuilder.conf.getServiceUrl(), "pulsar://service:6650");
        assertEquals(b2.conf.getServiceUrl(), "pulsar://other-broker:6650");
    }

    @Test
    public void enableTlsTest() {
        ClientBuilderImpl builder = (ClientBuilderImpl)PulsarClient.builder().serviceUrl("pulsar://service:6650");
        assertEquals(builder.conf.isUseTls(), false);
        assertEquals(builder.conf.getServiceUrl(), "pulsar://service:6650");

        builder = (ClientBuilderImpl)PulsarClient.builder().serviceUrl("http://service:6650");
        assertEquals(builder.conf.isUseTls(), false);
        assertEquals(builder.conf.getServiceUrl(), "http://service:6650");

        builder = (ClientBuilderImpl)PulsarClient.builder().serviceUrl("pulsar+ssl://service:6650");
        assertEquals(builder.conf.isUseTls(), true);
        assertEquals(builder.conf.getServiceUrl(), "pulsar+ssl://service:6650");

        builder = (ClientBuilderImpl)PulsarClient.builder().serviceUrl("https://service:6650");
        assertEquals(builder.conf.isUseTls(), true);
        assertEquals(builder.conf.getServiceUrl(), "https://service:6650");

        builder = (ClientBuilderImpl)PulsarClient.builder().serviceUrl("pulsar://service:6650").enableTls(true);
        assertEquals(builder.conf.isUseTls(), true);
        assertEquals(builder.conf.getServiceUrl(), "pulsar://service:6650");

        builder = (ClientBuilderImpl)PulsarClient.builder().serviceUrl("pulsar+ssl://service:6650").enableTls(false);
        assertEquals(builder.conf.isUseTls(), true);
        assertEquals(builder.conf.getServiceUrl(), "pulsar+ssl://service:6650");
    }

    @Test
    public void readerBuilderLoadConfTest() throws Exception {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        String topicName = "test_src";
        MessageId messageId = new MessageIdImpl(1, 2, 3);
        Map<String, Object> config = new HashMap<>();
        config.put("topicName", topicName);
        config.put("receiverQueueSize", 2000);
        ReaderBuilderImpl<byte[]> builder = (ReaderBuilderImpl<byte[]>) client.newReader()
            .startMessageId(messageId)
            .loadConf(config);

        Class<?> clazz = builder.getClass();
        Field conf = clazz.getDeclaredField("conf");
        conf.setAccessible(true);
        Object obj = conf.get(builder);
        assertTrue(obj instanceof ReaderConfigurationData);
        if (obj instanceof ReaderConfigurationData) {
            assertEquals(((ReaderConfigurationData) obj).getTopicName(), topicName);
            assertEquals(((ReaderConfigurationData) obj).getStartMessageId(), messageId);
        }
    }
}
