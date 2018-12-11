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
package org.apache.pulsar.io.netty.tcp.server;

import org.apache.pulsar.io.netty.NettyTcpSource;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests for Netty Tcp Server
 */
public class NettyTcpServerTest {

    @Test
    public void testNettyTcpServerConstructor() {
        NettyTcpServer<String> nettyTcpServer = new NettyTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setNumberOfThreads(2)
                .setNettyTcpSource(new NettyTcpSource())
                .build();

        assertNotNull(nettyTcpServer);

        try {
            nettyTcpServer.run();
        } catch (Exception ex) {
            fail("Unexpected exception handled when nettyTcpServer runs: " + ex.getMessage());
        } finally {
            nettyTcpServer.shutdownGracefully();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNettyTcpServerConstructorWhenHostIsNotSet() {
        new NettyTcpServer.Builder<String>()
                .setPort(10999)
                .setNumberOfThreads(2)
                .setNettyTcpSource(new NettyTcpSource())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNettyTcpServerConstructorWhenPortIsNotSet() {
        new NettyTcpServer.Builder<String>()
                .setHost("localhost")
                .setNumberOfThreads(2)
                .setNettyTcpSource(new NettyTcpSource())
                .build();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testNettyTcpServerConstructorWhenNumberOfThreadsIsNotSet() {
        new NettyTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setNettyTcpSource(new NettyTcpSource())
                .build();
    }


    @Test(expected = NullPointerException.class)
    public void testNettyTcpServerConstructorWhenNettyTcpSourceIsNotSet() {
        new NettyTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setNumberOfThreads(2)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNettyTcpServerWhenHostIsSetAsBlank() {
        new NettyTcpServer.Builder<String>()
                .setHost(" ")
                .setPort(10999)
                .setNumberOfThreads(2)
                .setNettyTcpSource(new NettyTcpSource())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNettyTcpServerWhenPortIsSetAsZero() {
        new NettyTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(0)
                .setNumberOfThreads(2)
                .setNettyTcpSource(new NettyTcpSource())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNettyTcpServerWhenPortIsSetLowerThan1024() {
        new NettyTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(1022)
                .setNumberOfThreads(2)
                .setNettyTcpSource(new NettyTcpSource())
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNettyTcpServerWhenNumberOfThreadsIsSetAsZero() {
        new NettyTcpServer.Builder<String>()
                .setHost("localhost")
                .setPort(10999)
                .setNumberOfThreads(0)
                .setNettyTcpSource(new NettyTcpSource())
                .build();
    }

}
