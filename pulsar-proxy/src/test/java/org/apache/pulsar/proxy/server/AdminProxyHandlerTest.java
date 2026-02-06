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
package org.apache.pulsar.proxy.server;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.client.api.Authentication;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AdminProxyHandlerTest {
    private AdminProxyHandler adminProxyHandler;

    @BeforeClass
    public void setupMocks() throws ServletException {
        // given
        HttpClient httpClient = mock(HttpClient.class);
        adminProxyHandler = new AdminProxyHandler(mock(ProxyConfiguration.class),
                mock(BrokerDiscoveryProvider.class), mock(Authentication.class)) {
            @Override
            protected HttpClient createHttpClient() throws ServletException {
                return httpClient;
            }
        };
        ServletConfig servletConfig = mock(ServletConfig.class);
        when(servletConfig.getServletName()).thenReturn("AdminProxyHandler");
        when(servletConfig.getServletContext()).thenReturn(mock(ServletContext.class));
        adminProxyHandler.init(servletConfig);
    }

    @Test
    public void testRequestTimeout() {
        ProxyConfiguration proxyConfiguration = spy(new ProxyConfiguration());
        proxyConfiguration.setHttpProxyTimeout(120 * 1000);

        adminProxyHandler = new AdminProxyHandler(proxyConfiguration,
                mock(BrokerDiscoveryProvider.class), mock(Authentication.class));

        HttpClient httpClient = mock(HttpClient.class);
        adminProxyHandler.customizeHttpClient(httpClient);

        assertEquals(adminProxyHandler.getTimeout(), 120 * 1000);
    }

    @Test
    public void replayableProxyContentProviderTest() throws Exception {
        HttpServletRequest request = mock(HttpServletRequest.class);
        doReturn(-1).when(request).getContentLength();

        try {
            AdminProxyHandler.ReplayableProxyContentProvider replayableProxyContentProvider =
                    adminProxyHandler.new ReplayableProxyContentProvider(
                            request, mock(HttpServletResponse.class), mock(Request.class), mock(InputStream.class),
                            1024);
            Field field = replayableProxyContentProvider.getClass().getDeclaredField("bodyBuffer");
            field.setAccessible(true);
            Assert.assertEquals(((ByteArrayOutputStream) field.get(replayableProxyContentProvider)).size(), 0);
        } catch (IllegalArgumentException e) {
            Assert.fail("IllegalArgumentException should not be thrown");
        }

    }

    @Test
    public void shouldLimitReplayBodyBufferSize() throws Exception {
        HttpServletRequest request = mock(HttpServletRequest.class);
        int maxRequestBodySize = 1024 * 1024;
        int requestBodySize = maxRequestBodySize + 1;
        doReturn(requestBodySize).when(request).getContentLength();
        byte[] inputBuffer = new byte[requestBodySize];

        AdminProxyHandler.ReplayableProxyContentProvider replayableProxyContentProvider =
                adminProxyHandler.new ReplayableProxyContentProvider(request, mock(HttpServletResponse.class),
                        mock(Request.class), new ByteArrayInputStream(inputBuffer),
                        maxRequestBodySize);

        // when

        // content is consumed
        Iterator<ByteBuffer> byteBufferIterator = replayableProxyContentProvider.iterator();
        int consumedBytes = 0;
        while (byteBufferIterator.hasNext()) {
            ByteBuffer byteBuffer = byteBufferIterator.next();
            consumedBytes += byteBuffer.limit();
        }

        // then
        assertEquals(consumedBytes, requestBodySize);
        Field field = replayableProxyContentProvider.getClass().getDeclaredField("bodyBufferMaxSizeReached");
        field.setAccessible(true);
        assertEquals(((boolean) field.get(replayableProxyContentProvider)), true);
    }

    @Test
    public void shouldReplayBodyBuffer() {
        // given
        HttpServletRequest request = mock(HttpServletRequest.class);
        int maxRequestBodySize = 1024 * 1024;
        byte[] inputBuffer = new byte[maxRequestBodySize - 1];
        for (int i = 0; i < inputBuffer.length; i++) {
            inputBuffer[i] = (byte) (i & 0xff);
        }
        doReturn(inputBuffer.length).when(request).getContentLength();

        AdminProxyHandler.ReplayableProxyContentProvider replayableProxyContentProvider =
                adminProxyHandler.new ReplayableProxyContentProvider(request, mock(HttpServletResponse.class),
                        mock(Request.class), new ByteArrayInputStream(inputBuffer),
                        maxRequestBodySize);

        ByteBuffer consumeBuffer = ByteBuffer.allocate(maxRequestBodySize);
        // content can be consumed multiple times
        for (int i = 0; i < 3; i++) {
            // when
            consumeBuffer.clear();
            Iterator<ByteBuffer> byteBufferIterator = replayableProxyContentProvider.iterator();
            while (byteBufferIterator.hasNext()) {
                ByteBuffer byteBuffer = byteBufferIterator.next();
                consumeBuffer.put(byteBuffer);
            }
            consumeBuffer.flip();
            byte[] consumedBytes = new byte[consumeBuffer.limit()];
            consumeBuffer.get(consumedBytes);
            // then
            assertEquals(consumedBytes, inputBuffer, "i=" + i);
        }
    }
}
