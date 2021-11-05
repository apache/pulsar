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
package org.apache.pulsar.broker.protocol;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.testng.annotations.Test;

/**
 * Unit test {@link ProtocolHandlerWithClassLoader}.
 */
@Test(groups = "broker")
public class ProtocolHandlerWithClassLoaderTest {

    @Test
    public void testWrapper() throws Exception {
        ProtocolHandler h = mock(ProtocolHandler.class);
        NarClassLoader loader = mock(NarClassLoader.class);
        ProtocolHandlerWithClassLoader wrapper = new ProtocolHandlerWithClassLoader(h, loader);

        String protocol = "kafka";

        when(h.protocolName()).thenReturn(protocol);
        assertEquals(protocol, wrapper.protocolName());
        verify(h, times(1)).protocolName();

        when(h.accept(eq(protocol))).thenReturn(true);
        assertTrue(wrapper.accept(protocol));
        verify(h, times(1)).accept(same(protocol));

        ServiceConfiguration conf = new ServiceConfiguration();
        wrapper.initialize(conf);
        verify(h, times(1)).initialize(same(conf));

        BrokerService service = mock(BrokerService.class);
        wrapper.start(service);
        verify(h, times(1)).start(service);

        String protocolData = "test-protocol-data";
        when(h.getProtocolDataToAdvertise()).thenReturn(protocolData);
        assertEquals(protocolData, wrapper.getProtocolDataToAdvertise());
        verify(h, times(1)).getProtocolDataToAdvertise();
    }

    @Test
    public void testClassLoaderSwitcher() throws Exception {
        NarClassLoader loader = mock(NarClassLoader.class);

        String protocol = "test-protocol";

        ProtocolHandler h = new ProtocolHandler() {
            @Override
            public String protocolName() {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
                return protocol;
            }

            @Override
            public boolean accept(String protocol) {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
                return true;
            }

            @Override
            public void initialize(ServiceConfiguration conf) throws Exception {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
                throw new Exception("test exception");
            }

            @Override
            public String getProtocolDataToAdvertise() {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
                return "test-protocol-data";
            }

            @Override
            public void start(BrokerService service) {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
            }

            @Override
            public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
                return null;
            }

            @Override
            public void close() {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
            }
        };
        ProtocolHandlerWithClassLoader wrapper = new ProtocolHandlerWithClassLoader(h, loader);

        ClassLoader curClassLoader = Thread.currentThread().getContextClassLoader();

        assertEquals(wrapper.protocolName(), protocol);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);

        assertTrue(wrapper.accept(protocol));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);


        ServiceConfiguration conf = new ServiceConfiguration();
        expectThrows(Exception.class, () -> wrapper.initialize(conf));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);

        assertEquals(wrapper.getProtocolDataToAdvertise(), "test-protocol-data");
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);

        BrokerService service = mock(BrokerService.class);
        wrapper.start(service);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);


        assertNull(wrapper.newChannelInitializers());
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);

        wrapper.close();
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
    }
}
