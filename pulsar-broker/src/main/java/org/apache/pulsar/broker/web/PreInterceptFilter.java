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
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.common.intercept.InterceptException;

@Slf4j
public class PreInterceptFilter implements Filter {

    private final BrokerInterceptor interceptor;

    private final ExceptionHandler exceptionHandler;

    public PreInterceptFilter(BrokerInterceptor interceptor, ExceptionHandler exceptionHandler) {
        this.interceptor = interceptor;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
                         FilterChain filterChain) throws IOException, ServletException {
        if (log.isDebugEnabled()) {
            log.debug("PreInterceptFilter: path {}, type {}",
                    servletRequest.getServletContext().getContextPath(),
                    servletRequest.getContentType());
        }
        if (StringUtils.containsIgnoreCase(servletRequest.getContentType(), MediaType.MULTIPART_FORM_DATA)
                || StringUtils.containsIgnoreCase(servletRequest.getContentType(),
                MediaType.APPLICATION_OCTET_STREAM)) {
            // skip multipart request at this moment
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }
        try {
            RequestWrapper requestWrapper = new RequestWrapper((HttpServletRequest) servletRequest);
            if (interceptor != null) {
                interceptor.onWebserviceRequest(requestWrapper);
            }
            filterChain.doFilter(requestWrapper, servletResponse);
        } catch (InterceptException e) {
            exceptionHandler.handle(servletResponse, e);
        }
    }

    @Override
    public void destroy() {

    }
}
