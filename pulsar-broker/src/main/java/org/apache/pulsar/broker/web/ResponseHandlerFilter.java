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
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet filter that hooks up to handle outgoing response.
 */
public class ResponseHandlerFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(ResponseHandlerFilter.class);
    private static final String BROKER_ADDRESS_HEADER_NAME = "broker-address";

    private final String brokerAddress;
    private final BrokerInterceptor interceptor;
    private final boolean interceptorEnabled;

    public ResponseHandlerFilter(PulsarService pulsar) {
        this.brokerAddress = pulsar.getAdvertisedAddress();
        this.interceptor = pulsar.getBrokerInterceptor();
        this.interceptorEnabled = !pulsar.getConfig().getBrokerInterceptors().isEmpty();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        if (!response.isCommitted()) {
            ((HttpServletResponse) response).addHeader(BROKER_ADDRESS_HEADER_NAME, brokerAddress);
        } else {
            LOG.warn("Cannot add header {} to request {} since it's already committed.", BROKER_ADDRESS_HEADER_NAME,
                    request);
        }
        chain.doFilter(request, response);
        if (((HttpServletResponse) response).getStatus() == Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            // invalidate current session from servlet-container if it received internal-server-error
            try {
                ((HttpServletRequest) request).getSession(false).invalidate();
            } catch (Exception ignoreException) {
                /* connection is already invalidated */
            }
        }

        if (request.isAsyncSupported() && request.isAsyncStarted()) {
            request.getAsyncContext().addListener(new AsyncListener() {
                @Override
                public void onComplete(AsyncEvent asyncEvent) throws IOException {
                    handleInterceptor(request, response);
                }

                @Override
                public void onTimeout(AsyncEvent asyncEvent) throws IOException {
                    LOG.warn("Http request {} async context timeout.", request);
                    handleInterceptor(request, response);
                }

                @Override
                public void onError(AsyncEvent asyncEvent) throws IOException {
                    LOG.warn("Http request {} async context error.", request, asyncEvent.getThrowable());
                    handleInterceptor(request, response);
                }

                @Override
                public void onStartAsync(AsyncEvent asyncEvent) throws IOException {
                    // nothing to do
                }
            });
        } else {
            handleInterceptor(request, response);
        }
    }

    private void handleInterceptor(ServletRequest request, ServletResponse response) {
        if (interceptorEnabled
                && !StringUtils.containsIgnoreCase(request.getContentType(), MediaType.MULTIPART_FORM_DATA)
                && !StringUtils.containsIgnoreCase(request.getContentType(), MediaType.APPLICATION_OCTET_STREAM)) {
            try {
                interceptor.onWebserviceResponse(request, response);
            } catch (Exception e) {
                LOG.error("Failed to handle interceptor on web service response.", e);
            }
        }
    }

    @Override
    public void init(FilterConfig arg) throws ServletException {
        // No init necessary.
    }

    @Override
    public void destroy() {
        // No state to clean up.
    }

}
