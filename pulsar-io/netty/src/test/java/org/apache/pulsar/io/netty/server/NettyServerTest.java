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
package org.apache.pulsar.io.netty.server;

import org.apache.pulsar.io.netty.NettySource;
import org.apache.pulsar.io.netty.NettySourceConfig;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertNotNull;

/**
 * Tests for Netty Tcp or Udp Server
 */
public class NettyServerTest {

    private static final String LOCALHOST = "127.0.0.1";
    private static final String TCP = "TCP";
    private static final String UDP = "UDP";

    @Test
    public void testNettyTcpServerConstructor() {
        NettyServer nettyTcpServer = new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(TCP))
                .setHost(LOCALHOST)
                .setPort(10999)
                .setNumberOfThreads(2)
                .setNettySource(new NettySource())
                .build();

        assertNotNull(nettyTcpServer);
    }

    @Test
    public void testNettyUdpServerConstructor() {
        NettyServer nettyUdpServer = new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(UDP))
                .setHost(LOCALHOST)
                .setPort(10999)
                .setNumberOfThreads(2)
                .setNettySource(new NettySource())
                .build();

        assertNotNull(nettyUdpServer);
    }

    @Test
    public void testNettyTcpServerByNettySourceConfig() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("type", "tcp");
        map.put("host", LOCALHOST);
        map.put("port", 10999);
        map.put("numberOfThreads", 1);

        NettySourceConfig nettySourceConfig = NettySourceConfig.load(map);

        // test NettySource run function NettyServer Builder
        NettyServer nettyTcpServer = new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(nettySourceConfig.getType().toUpperCase()))
                .setHost(nettySourceConfig.getHost())
                .setPort(nettySourceConfig.getPort())
                .setNumberOfThreads(nettySourceConfig.getNumberOfThreads())
                .setNettySource(new NettySource())
                .build();

        assertNotNull(nettyTcpServer);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNettyTcpServerConstructorWhenHostIsNotSet() {
        new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(TCP))
                .setPort(10999)
                .setNumberOfThreads(2)
                .setNettySource(new NettySource())
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNettyTcpServerConstructorWhenPortIsNotSet() {
        new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(TCP))
                .setHost(LOCALHOST)
                .setNumberOfThreads(2)
                .setNettySource(new NettySource())
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNettyTcpServerConstructorWhenNumberOfThreadsIsNotSet() {
        new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(TCP))
                .setHost(LOCALHOST)
                .setPort(10999)
                .setNettySource(new NettySource())
                .build();
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNettyTcpServerConstructorWhenNettyTcpSourceIsNotSet() {
        new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(TCP))
                .setHost(LOCALHOST)
                .setPort(10999)
                .setNumberOfThreads(2)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNettyTcpServerWhenHostIsSetAsBlank() {
        new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(TCP))
                .setHost(" ")
                .setPort(10999)
                .setNumberOfThreads(2)
                .setNettySource(new NettySource())
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNettyTcpServerWhenPortIsSetAsZero() {
        new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(TCP))
                .setHost(LOCALHOST)
                .setPort(0)
                .setNumberOfThreads(2)
                .setNettySource(new NettySource())
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNettyTcpServerWhenPortIsSetLowerThan1024() {
        new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(TCP))
                .setHost(LOCALHOST)
                .setPort(1022)
                .setNumberOfThreads(2)
                .setNettySource(new NettySource())
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNettyTcpServerWhenNumberOfThreadsIsSetAsZero() {
        new NettyServer.Builder()
                .setType(NettyServer.Type.valueOf(TCP))
                .setHost(LOCALHOST)
                .setPort(10999)
                .setNumberOfThreads(0)
                .setNettySource(new NettySource())
                .build();
    }

}
