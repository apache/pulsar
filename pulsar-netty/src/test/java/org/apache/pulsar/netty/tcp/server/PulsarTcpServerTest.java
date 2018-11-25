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
package org.apache.pulsar.netty.tcp.server;

import io.netty.handler.codec.string.StringDecoder;
import org.apache.pulsar.netty.serde.PulsarStringSerializer;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Tests for Pulsar Tcp Server
 */
public class PulsarTcpServerTest {

    @Test
    public void testPulsarTcpServerConstructor() {
        PulsarTcpServer<String> pulsarTcpServer = new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();

        assertNotNull(pulsarTcpServer);
    }

    @Test(expected = NullPointerException.class)
    public void testPulsarTcpServerConstructorWhenHostIsNotSet() {
        new PulsarTcpServer.Builder<String>()
                .setPort(10999)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarTcpServerConstructorWhenPortIsNotSet() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testPulsarTcpServerConstructorWhenServiceUrlIsNotSet() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testPulsarTcpServerConstructorWhenTopicNameIsNotSet() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarTcpServerConstructorWhenNumberOfThreadsIsNotSet() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test
    public void testPulsarTcpServerConstructorWhenDecoderIsNotSet() {
        PulsarTcpServer<String> pulsarTcpServer = new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();

        assertNotNull(pulsarTcpServer);
    }

    @Test(expected = NullPointerException.class)
    public void testPulsarTcpServerConstructorWhenPulsarSerializerIsNotSet() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarTcpServerWhenHostIsSetAsBlank() {
        new PulsarTcpServer.Builder<String>()
                .setHost(" ")
                .setPort(10999)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarTcpServerWhenPortIsSetAsZero() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(0)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarTcpServerWhenPortIsSetLowerThan1024() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(1022)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarTcpServerWhenServiceUrlIsSetAsBlank() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setServiceUrl(" ")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarTcpServerWhenTopicNameIsSetAsBlank() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName(" ")
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarTcpServerWhenNumberOfThreadsIsSetAsZero() {
        new PulsarTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setServiceUrl("pulsar://127.0.0.1:6650")
                .setTopicName("my-netty-topic")
                .setNumberOfThreads(0)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new PulsarStringSerializer())
                .build();
    }
}
