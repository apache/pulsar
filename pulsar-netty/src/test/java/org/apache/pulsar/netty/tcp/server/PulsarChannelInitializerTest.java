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

import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import org.apache.pulsar.netty.serde.PulsarStringSerializer;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for Pulsar Channel Initializer
 */
public class PulsarChannelInitializerTest {

    @Test
    public void testGenericChannelInitializerWhenDecoderIsSet() throws Exception {
        NioSocketChannel channel = new NioSocketChannel();

        PulsarChannelInitializer pulsarChannelInitializer = new PulsarChannelInitializer(
                Optional.ofNullable(new StringDecoder()),
                new PulsarTcpServerHandler<>("testServiceUrl", "testTopic", new PulsarStringSerializer()));
        pulsarChannelInitializer.initChannel(channel);

        assertNotNull(channel.pipeline().toMap());
        assertEquals(2, channel.pipeline().toMap().size());
    }

    @Test
    public void testGenericChannelInitializerWhenDecoderIsNotSet() throws Exception {
        NioSocketChannel channel = new NioSocketChannel();

        PulsarChannelInitializer pulsarChannelInitializer = new PulsarChannelInitializer(
                Optional.empty(),
                new PulsarTcpServerHandler<>("testServiceUrl", "testTopic", new PulsarStringSerializer()));
        pulsarChannelInitializer.initChannel(channel);

        assertNotNull(channel.pipeline().toMap());
        assertEquals(2, channel.pipeline().toMap().size());
    }

}
