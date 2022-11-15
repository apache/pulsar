/*
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
package org.apache.pulsar.io.netty.udp;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.pulsar.io.netty.NettySource;
import org.testng.annotations.Test;

import io.netty.channel.socket.nio.NioDatagramChannel;

/**
 * Tests for Netty Channel Initializer
 */
public class NettyUDPChannelInitializerTest {

    @Test
    public void testChannelInitializer() throws Exception {
        NioDatagramChannel channel = new NioDatagramChannel();

        NettyUDPChannelInitializer nettyChannelInitializer = new NettyUDPChannelInitializer(
                new NettyUDPServerHandler(new NettySource()));
        nettyChannelInitializer.initChannel(channel);

        assertNotNull(channel.pipeline().toMap());
        assertEquals(1, channel.pipeline().toMap().size());
    }
    
}
