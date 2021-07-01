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
package org.apache.pulsar.broker.web;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

public class ProcessHandlerFilterTest {

    @Test
    public void testInterceptorOnFilter() throws ServletException, IOException {
        PulsarService mockPulsarService = Mockito.mock(PulsarService.class);
        BrokerInterceptor spyInterceptor = Mockito.spy(BrokerInterceptor.class);
        HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse mockHttpServletResponse = Mockito.mock(HttpServletResponse.class);
        ServiceConfiguration mockConfig = Mockito.mock(ServiceConfiguration.class);
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);
        Mockito.doReturn(spyInterceptor).when(mockPulsarService).getBrokerInterceptor();
        Mockito.doReturn(mockConfig).when(mockPulsarService).getConfig();
        Mockito.doReturn(Sets.newHashSet("Interceptor1", "Interceptor2")).when(mockConfig).getBrokerInterceptors();
        ProcessHandlerFilter processHandlerFilter = new ProcessHandlerFilter(mockPulsarService);
        processHandlerFilter.doFilter(mockHttpServletRequest, mockHttpServletResponse, mockFilterChain);
        Mockito.verify(spyInterceptor).onFilter(mockHttpServletRequest, mockHttpServletResponse, mockFilterChain);
    }

    @Test
    public void testChainDoFilter() throws ServletException, IOException {
        PulsarService mockPulsarService = Mockito.mock(PulsarService.class);
        BrokerInterceptor spyInterceptor = Mockito.mock(BrokerInterceptor.class);
        HttpServletResponse mockHttpServletResponse = Mockito.mock(HttpServletResponse.class);
        ServiceConfiguration mockConfig = Mockito.mock(ServiceConfiguration.class);
        FilterChain spyFilterChain = Mockito.spy(FilterChain.class);
        Mockito.doReturn(spyInterceptor).when(mockPulsarService).getBrokerInterceptor();
        Mockito.doReturn(mockConfig).when(mockPulsarService).getConfig();
        Mockito.doReturn(Sets.newHashSet()).when(mockConfig).getBrokerInterceptors();
        // empty interceptor list
        HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
        ProcessHandlerFilter processHandlerFilter = new ProcessHandlerFilter(mockPulsarService);
        processHandlerFilter.doFilter(mockHttpServletRequest, mockHttpServletResponse, spyFilterChain);
        Mockito.verify(spyFilterChain).doFilter(mockHttpServletRequest, mockHttpServletResponse);
        Mockito.clearInvocations(spyFilterChain);
        // request has MULTIPART_FORM_DATA content-type
        Mockito.doReturn(Sets.newHashSet("Interceptor1","Interceptor2")).when(mockConfig).getBrokerInterceptors();
        HttpServletRequest mockHttpServletRequest2 = Mockito.mock(HttpServletRequest.class);
        Mockito.doReturn(MediaType.MULTIPART_FORM_DATA).when(mockHttpServletRequest2).getContentType();
        ProcessHandlerFilter processHandlerFilter2 = new ProcessHandlerFilter(mockPulsarService);
        processHandlerFilter2.doFilter(mockHttpServletRequest2, mockHttpServletResponse, spyFilterChain);
        Mockito.verify(spyFilterChain).doFilter(mockHttpServletRequest2, mockHttpServletResponse);
        Mockito.clearInvocations(spyFilterChain);
        // request has APPLICATION_OCTET_STREAM content-type
        Mockito.doReturn(MediaType.APPLICATION_OCTET_STREAM).when(mockHttpServletRequest2).getContentType();
        processHandlerFilter2.doFilter(mockHttpServletRequest2, mockHttpServletResponse, spyFilterChain);
        Mockito.verify(spyFilterChain).doFilter(mockHttpServletRequest2, mockHttpServletResponse);
    }


}