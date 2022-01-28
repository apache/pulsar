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
package org.apache.pulsar.proxy.extensions;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Map;

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

/**
 * Unit test {@link ProxyExtensionWithClassLoader}.
 */
@Test(groups = "broker")
public class ProxyExtensionWithClassLoaderTest {

    @Test
    public void testWrapper() throws Exception {
        ProxyExtension h = mock(ProxyExtension.class);
        NarClassLoader loader = mock(NarClassLoader.class);
        ProxyExtensionWithClassLoader wrapper = new ProxyExtensionWithClassLoader(h, loader);

        String protocol = "kafka";

        when(h.extensionName()).thenReturn(protocol);
        assertEquals(protocol, wrapper.extensionName());
        verify(h, times(1)).extensionName();

        when(h.accept(eq(protocol))).thenReturn(true);
        assertTrue(wrapper.accept(protocol));
        verify(h, times(1)).accept(same(protocol));

        ProxyConfiguration conf = new ProxyConfiguration();
        wrapper.initialize(conf);
        verify(h, times(1)).initialize(same(conf));

        ProxyService service = mock(ProxyService.class);
        wrapper.start(service);
        verify(h, times(1)).start(service);
    }

    @Test
    public void testClassLoaderSwitcher() throws Exception {
        NarClassLoader loader = mock(NarClassLoader.class);

        String protocol = "test-protocol";

        ProxyExtension h = new ProxyExtension() {
            @Override
            public String extensionName() {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
                return protocol;
            }

            @Override
            public boolean accept(String protocol) {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
                return true;
            }

            @Override
            public void initialize(ProxyConfiguration conf) throws Exception {
                assertEquals(Thread.currentThread().getContextClassLoader(), loader);
                throw new Exception("test exception");
            }

            @Override
            public void start(ProxyService service) {
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
        ProxyExtensionWithClassLoader wrapper = new ProxyExtensionWithClassLoader(h, loader);

        ClassLoader curClassLoader = Thread.currentThread().getContextClassLoader();

        assertEquals(wrapper.extensionName(), protocol);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);

        assertTrue(wrapper.accept(protocol));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);


        ProxyConfiguration conf = new ProxyConfiguration();
        expectThrows(Exception.class, () -> wrapper.initialize(conf));
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);

        ProxyService service = mock(ProxyService.class);
        wrapper.start(service);
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);


        assertNull(wrapper.newChannelInitializers());
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);

        wrapper.close();
        assertEquals(Thread.currentThread().getContextClassLoader(), curClassLoader);
    }
}
