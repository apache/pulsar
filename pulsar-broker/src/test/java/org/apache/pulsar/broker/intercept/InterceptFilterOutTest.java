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
package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.web.ExceptionHandler;
import org.apache.pulsar.broker.web.PreInterceptFilter;
import org.apache.pulsar.broker.web.ProcessHandlerFilter;
import org.apache.pulsar.broker.web.ResponseHandlerFilter;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

import javax.servlet.FilterChain;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the the interceptor filter out.
 */
@Test(groups = "broker")
public class InterceptFilterOutTest {

    private static final String[] shouldBeFilterOutContentTypes = new String[] {
            "multipart/form-data",
            "Multipart/form-data",
            "multipart/form-data; boundary=------",
            "multipart/Form-data; boundary=------",
            "application/octet-stream",
            "application/Octet-stream",
            "application/octet-stream; xxx"
    };

    @Test
    public void testFilterOutForPreInterceptFilter() throws Exception {
        CounterBrokerInterceptor interceptor = new CounterBrokerInterceptor();
        ExceptionHandler handler = new ExceptionHandler();
        PreInterceptFilter filter = new PreInterceptFilter(interceptor, handler);

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        Mockito.doNothing().when(chain).doFilter(Mockito.any(), Mockito.any());
        HttpServletRequestWrapper mockInputStream = new MockRequestWrapper(request);
        Mockito.doReturn(mockInputStream.getInputStream()).when(request).getInputStream();
        Mockito.doReturn(new StringBuffer("http://127.0.0.1:8080")).when(request).getRequestURL();

        // "application/json" should be intercepted
        Mockito.doReturn("application/json").when(request).getContentType();
        filter.doFilter(request, response, chain);
        Assert.assertEquals(interceptor.getCount(), 1);

        for (String shouldBeFilterOutContentType : shouldBeFilterOutContentTypes) {
            Mockito.doReturn(shouldBeFilterOutContentType).when(request).getContentType();
            filter.doFilter(request, response, chain);
            Assert.assertEquals(interceptor.getCount(), 1);
        }
    }

    @Test
    public void testOnFilter() throws Exception {
        CounterBrokerInterceptor interceptor = new CounterBrokerInterceptor();
        PulsarService pulsarService = Mockito.mock(PulsarService.class);
        Mockito.doReturn("pulsar://127.0.0.1:6650").when(pulsarService).getAdvertisedAddress();
        Mockito.doReturn(interceptor).when(pulsarService).getBrokerInterceptor();
        ServiceConfiguration conf = Mockito.mock(ServiceConfiguration.class);
        Mockito.doReturn(Sets.newHashSet("interceptor")).when(conf).getBrokerInterceptors();
        Mockito.doReturn(conf).when(pulsarService).getConfig();
        //init filter
        ProcessHandlerFilter filter = new ProcessHandlerFilter(pulsarService);

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        Mockito.doNothing().when(chain).doFilter(Mockito.any(), Mockito.any());
        HttpServletRequestWrapper mockInputStream = new MockRequestWrapper(request);
        Mockito.doReturn(mockInputStream.getInputStream()).when(request).getInputStream();
        Mockito.doReturn(new StringBuffer("http://127.0.0.1:8080")).when(request).getRequestURL();
        // "application/json" should be intercepted
        Mockito.doReturn("application/json").when(request).getContentType();

        filter.doFilter(request, response, chain);
        Assert.assertEquals(interceptor.getCount(), 100);
        verify(chain, times(1)).doFilter(request, response);
    }

    @Test
    public void testFilterOutForResponseInterceptFilter() throws Exception {
        CounterBrokerInterceptor interceptor = new CounterBrokerInterceptor();
        PulsarService pulsarService = Mockito.mock(PulsarService.class);
        Mockito.doReturn("pulsar://127.0.0.1:6650").when(pulsarService).getAdvertisedAddress();
        Mockito.doReturn(interceptor).when(pulsarService).getBrokerInterceptor();
        ServiceConfiguration conf = Mockito.mock(ServiceConfiguration.class);
        Mockito.doReturn(Sets.newHashSet("interceptor")).when(conf).getBrokerInterceptors();
        Mockito.doReturn(conf).when(pulsarService).getConfig();
        ResponseHandlerFilter filter = new ResponseHandlerFilter(pulsarService);

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        Mockito.doNothing().when(chain).doFilter(Mockito.any(), Mockito.any());
        HttpServletRequestWrapper mockInputStream = new MockRequestWrapper(request);
        Mockito.doReturn(mockInputStream.getInputStream()).when(request).getInputStream();
        Mockito.doReturn(new StringBuffer("http://127.0.0.1:8080")).when(request).getRequestURL();

        // "application/json" should be intercepted
        Mockito.doReturn("application/json").when(request).getContentType();
        filter.doFilter(request, response, chain);
        Assert.assertEquals(interceptor.getCount(), 1);

        for (String shouldBeFilterOutContentType : shouldBeFilterOutContentTypes) {
            Mockito.doReturn(shouldBeFilterOutContentType).when(request).getContentType();
            filter.doFilter(request, response, chain);
            Assert.assertEquals(interceptor.getCount(), 1);
        }
    }

    @Test
    public void testShouldNotInterceptWhenInterceptorDisabled() throws Exception {
        CounterBrokerInterceptor interceptor = new CounterBrokerInterceptor();
        PulsarService pulsarService = Mockito.mock(PulsarService.class);
        Mockito.doReturn("pulsar://127.0.0.1:6650").when(pulsarService).getAdvertisedAddress();
        Mockito.doReturn(interceptor).when(pulsarService).getBrokerInterceptor();
        ServiceConfiguration conf = Mockito.mock(ServiceConfiguration.class);
        // Disable the broker interceptor
        Mockito.doReturn(Sets.newHashSet()).when(conf).getBrokerInterceptors();
        Mockito.doReturn(conf).when(pulsarService).getConfig();
        ResponseHandlerFilter filter = new ResponseHandlerFilter(pulsarService);

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        FilterChain chain = Mockito.mock(FilterChain.class);
        Mockito.doNothing().when(chain).doFilter(Mockito.any(), Mockito.any());
        HttpServletRequestWrapper mockInputStream = new MockRequestWrapper(request);
        Mockito.doReturn(mockInputStream.getInputStream()).when(request).getInputStream();
        Mockito.doReturn(new StringBuffer("http://127.0.0.1:8080")).when(request).getRequestURL();

        // Should not be intercepted since the broker interceptor disabled.
        Mockito.doReturn("application/json").when(request).getContentType();
        filter.doFilter(request, response, chain);
        Assert.assertEquals(interceptor.getCount(), 0);
    }

    private static class MockRequestWrapper extends HttpServletRequestWrapper {

        public MockRequestWrapper(HttpServletRequest request) {
            super(request);
            this.body = new byte[]{0, 1, 2, 3, 4, 5};
        }

        private final byte[] body;

        @Override
        public ServletInputStream getInputStream() {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override
                public boolean isFinished() {
                    return false;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener readListener) {

                }

                public int read() {
                    return byteArrayInputStream.read();
                }
            };
        }

        @Override
        public BufferedReader getReader() throws IOException {
            return new BufferedReader(new InputStreamReader(this.getInputStream(), Charset.defaultCharset()));
        }
    }
}
