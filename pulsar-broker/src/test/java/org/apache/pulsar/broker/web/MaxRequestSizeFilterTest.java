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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MaxRequestSizeFilterTest {
    private static final long MAX_SIZE = 2;
    private static final long LEGAL_SIZE = 1;
    private static final long ILLEGAL_SIZE = 3;

    @Test
    public void testInChunkedReturnFalse()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        MaxRequestSizeFilter maxRequestSizeFilter = new MaxRequestSizeFilter(MAX_SIZE);
        Method isChunked = maxRequestSizeFilter.getClass()
                .getDeclaredMethod("isChunked", ServletRequest.class);
        isChunked.setAccessible(true);
        // request is not httpServlet Request
        ServletRequest mockHttpServletRequest = Mockito.mock(ServletRequest.class);
        Boolean result = (Boolean) isChunked.invoke(maxRequestSizeFilter, mockHttpServletRequest);
        Assert.assertFalse(result);
        // request not include encoding
        HttpServletRequest spyHttpServletRequest = Mockito.spy(HttpServletRequest.class);
        Mockito.doReturn(null).when(spyHttpServletRequest).getHeader("Transfer-Encoding");
        Boolean result2 = (Boolean) isChunked.invoke(maxRequestSizeFilter, spyHttpServletRequest);
        Assert.assertFalse(result2);
        //request Transfer-Encoding is not chunked
        HttpServletRequest spyHttpServletRequest3 = Mockito.spy(HttpServletRequest.class);
        Mockito.doReturn("whatever").when(spyHttpServletRequest3).getHeader("Transfer-Encoding");
        Boolean result3 = (Boolean) isChunked.invoke(maxRequestSizeFilter, spyHttpServletRequest);
        Assert.assertFalse(result3);
    }

    @Test
    public void testInChunkedReturnTrue()
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        MaxRequestSizeFilter maxRequestSizeFilter = new MaxRequestSizeFilter(MAX_SIZE);
        Method isChunked = maxRequestSizeFilter.getClass()
                .getDeclaredMethod("isChunked", ServletRequest.class);
        isChunked.setAccessible(true);
        HttpServletRequest spyHttpServletRequest = Mockito.spy(HttpServletRequest.class);
        // request  Transfer-Encoding is chunked
        Mockito.doReturn("chunked").when(spyHttpServletRequest).getHeader("Transfer-Encoding");
        Boolean result = (Boolean) isChunked.invoke(maxRequestSizeFilter, spyHttpServletRequest);
        Assert.assertTrue(result);
    }

    @Test
    public void testDoFilterSendError() throws ServletException, IOException {
        MaxRequestSizeFilter maxRequestSizeFilter = new MaxRequestSizeFilter(MAX_SIZE);
        FilterChain mockFilterChain = Mockito.mock(FilterChain.class);
        // the size grater than max size
        HttpServletRequest spyHttpServletRequest = Mockito.spy(HttpServletRequest.class);
        HttpServletResponse spyHttpServletResponse = Mockito.spy(HttpServletResponse.class);
        Mockito.doReturn(ILLEGAL_SIZE).when(spyHttpServletRequest).getContentLengthLong();
        maxRequestSizeFilter.doFilter(spyHttpServletRequest, spyHttpServletResponse, mockFilterChain);
        Mockito.verify(spyHttpServletResponse).sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad Request");
        // the request is chunked
        HttpServletRequest spyHttpServletRequest2 = Mockito.spy(HttpServletRequest.class);
        HttpServletResponse spyHttpServletResponse2 = Mockito.spy(HttpServletResponse.class);
        Mockito.doReturn(LEGAL_SIZE).when(spyHttpServletRequest2).getContentLengthLong();
        Mockito.doReturn("chunked").when(spyHttpServletRequest2).getHeader("Transfer-Encoding");
        maxRequestSizeFilter.doFilter(spyHttpServletRequest2, spyHttpServletResponse2, mockFilterChain);
        Mockito.verify(spyHttpServletResponse).sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad Request");
    }

    @Test
    public void testDoFilterInvokeChainDoFilter() throws ServletException, IOException {
        MaxRequestSizeFilter maxRequestSizeFilter = new MaxRequestSizeFilter(MAX_SIZE);
        FilterChain spyFilterChain = Mockito.spy(FilterChain.class);
        ServletRequest spyHttpServletRequest = Mockito.spy(ServletRequest.class);
        ServletResponse spyHttpServletResponse = Mockito.spy(ServletResponse.class);
        Mockito.doReturn(LEGAL_SIZE).when(spyHttpServletRequest).getContentLengthLong();
        maxRequestSizeFilter.doFilter(spyHttpServletRequest, spyHttpServletResponse, spyFilterChain);
        Mockito.verify(spyFilterChain).doFilter(spyHttpServletRequest,spyHttpServletResponse);
    }
}