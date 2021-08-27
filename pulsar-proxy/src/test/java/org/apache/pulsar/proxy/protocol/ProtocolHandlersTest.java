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
package org.apache.pulsar.proxy.protocol;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

/**
 * Unit test {@link ProtocolHandlers}.
 */
@Test(groups = "proxy")
public class ProtocolHandlersTest {

    private static final String protocol1 = "protocol1";
    private ProtocolHandler handler1;
    private NarClassLoader ncl1;
    private static final String protocol2 = "protocol2";
    private ProtocolHandler handler2;
    private NarClassLoader ncl2;
    private static final String protocol3 = "protocol3";

    private Map<String, ProtocolHandlerWithClassLoader> handlerMap;
    private ProtocolHandlers handlers;

    @BeforeMethod
    public void setup() {
        this.handler1 = mock(ProtocolHandler.class);
        this.ncl1 = mock(NarClassLoader.class);
        this.handler2 = mock(ProtocolHandler.class);
        this.ncl2 = mock(NarClassLoader.class);

        this.handlerMap = new HashMap<>();
        this.handlerMap.put(
            protocol1,
            new ProtocolHandlerWithClassLoader(handler1, ncl1));
        this.handlerMap.put(
            protocol2,
            new ProtocolHandlerWithClassLoader(handler2, ncl2));
        this.handlers = new ProtocolHandlers(this.handlerMap);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        this.handlers.close();

        verify(handler1, times(1)).close();
        verify(handler2, times(1)).close();
        verify(ncl1, times(1)).close();
        verify(ncl2, times(1)).close();
    }

    @Test
    public void testGetProtocol() {
        assertSame(handler1, handlers.protocol(protocol1));
        assertSame(handler2, handlers.protocol(protocol2));
        assertNull(handlers.protocol(protocol3));
    }

    @Test
    public void testInitialize() throws Exception {
        ProxyConfiguration conf = new ProxyConfiguration();
        handlers.initialize(conf);
        verify(handler1, times(1)).initialize(same(conf));
        verify(handler2, times(1)).initialize(same(conf));
    }

    @Test
    public void testStart() {
        ProxyService service = mock(ProxyService.class);
        handlers.start(service);
        verify(handler1, times(1)).start(same(service));
        verify(handler2, times(1)).start(same(service));
    }

    @Test
    public void testNewChannelInitializersSuccess() {
        ChannelInitializer<SocketChannel> i1 = mock(ChannelInitializer.class);
        ChannelInitializer<SocketChannel> i2 = mock(ChannelInitializer.class);
        Map<InetSocketAddress, ChannelInitializer<SocketChannel>> p1Initializers = new HashMap<>();
        p1Initializers.put(new InetSocketAddress("127.0.0.1", 6650), i1);
        p1Initializers.put(new InetSocketAddress("127.0.0.2", 6651), i2);

        ChannelInitializer<SocketChannel> i3 = mock(ChannelInitializer.class);
        ChannelInitializer<SocketChannel> i4 = mock(ChannelInitializer.class);
        Map<InetSocketAddress, ChannelInitializer<SocketChannel>> p2Initializers = new HashMap<>();
        p2Initializers.put(new InetSocketAddress("127.0.0.3", 6650), i3);
        p2Initializers.put(new InetSocketAddress("127.0.0.4", 6651), i4);

        when(handler1.newChannelInitializers()).thenReturn(p1Initializers);
        when(handler2.newChannelInitializers()).thenReturn(p2Initializers);

        Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> initializers =
            handlers.newChannelInitializers();

        assertEquals(2, initializers.size());
        assertSame(p1Initializers, initializers.get(protocol1));
        assertSame(p2Initializers, initializers.get(protocol2));
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testNewChannelInitializersOverlapped() {
        ChannelInitializer<SocketChannel> i1 = mock(ChannelInitializer.class);
        ChannelInitializer<SocketChannel> i2 = mock(ChannelInitializer.class);
        Map<InetSocketAddress, ChannelInitializer<SocketChannel>> p1Initializers = new HashMap<>();
        p1Initializers.put(new InetSocketAddress("127.0.0.1", 6650), i1);
        p1Initializers.put(new InetSocketAddress("127.0.0.2", 6651), i2);

        ChannelInitializer<SocketChannel> i3 = mock(ChannelInitializer.class);
        ChannelInitializer<SocketChannel> i4 = mock(ChannelInitializer.class);
        Map<InetSocketAddress, ChannelInitializer<SocketChannel>> p2Initializers = new HashMap<>();
        p2Initializers.put(new InetSocketAddress("127.0.0.1", 6650), i3);
        p2Initializers.put(new InetSocketAddress("127.0.0.4", 6651), i4);

        when(handler1.newChannelInitializers()).thenReturn(p1Initializers);
        when(handler2.newChannelInitializers()).thenReturn(p2Initializers);

        handlers.newChannelInitializers();
    }

}
