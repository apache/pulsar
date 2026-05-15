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
package org.apache.pulsar.broker.web;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
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
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;

/**
 * Servlet filter that hooks up to handle outgoing response.
 */
@CustomLog
public class ResponseHandlerFilter implements Filter {
    private static final String BROKER_ADDRESS_HEADER_NAME = "broker-address";

    private final String brokerAddress;
    private final BrokerInterceptor interceptor;

    public ResponseHandlerFilter(PulsarService pulsar) {
        this.brokerAddress = pulsar.getAdvertisedAddress();
        this.interceptor = Objects.requireNonNull(pulsar.getBrokerInterceptor());
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        if (!response.isCommitted()) {
            ((HttpServletResponse) response).addHeader(BROKER_ADDRESS_HEADER_NAME, brokerAddress);
        } else {
            log.warn()
                    .attr("header", BROKER_ADDRESS_HEADER_NAME)
                    .attr("request", request)
                    .log("Cannot add header to request since it's already committed.");
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
                public void onComplete(AsyncEvent asyncEvent) {
                    handleInterceptor(request, response);
                }

                @Override
                public void onTimeout(AsyncEvent asyncEvent) {
                    log.warn().attr("request", request).log("Http request async context timeout.");
                    handleInterceptor(request, response);
                }

                @Override
                public void onError(AsyncEvent asyncEvent) {
                    log.warn()
                            .attr("request", request)
                            .exceptionMessage(asyncEvent.getThrowable())
                            .log("Http request async context error.");
                    handleInterceptor(request, response);
                }

                @Override
                public void onStartAsync(AsyncEvent asyncEvent) {
                    // nothing to do
                }
            });
        } else {
            handleInterceptor(request, response);
        }
    }

    private void handleInterceptor(ServletRequest request, ServletResponse response) {
        String contentType = request.getContentType();
        if (contentType == null
                || (!contentType.toLowerCase(Locale.ROOT).contains(
                        MediaType.MULTIPART_FORM_DATA.toLowerCase(Locale.ROOT))
                && !contentType.toLowerCase(Locale.ROOT).contains(
                        MediaType.APPLICATION_OCTET_STREAM.toLowerCase(Locale.ROOT)))) {
            try {
                interceptor.onWebserviceResponse(request, response);
            } catch (Exception e) {
                log.error().exception(e).log("Failed to handle interceptor on web service response.");
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
