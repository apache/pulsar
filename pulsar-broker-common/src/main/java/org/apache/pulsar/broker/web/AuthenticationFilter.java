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
import javax.naming.AuthenticationException;
import javax.servlet.AsyncContext;
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
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet filter that hooks up with AuthenticationService to reject unauthenticated HTTP requests.
 */
public class AuthenticationFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationFilter.class);

    private final AuthenticationService authenticationService;

    public static final String AuthenticatedRoleAttributeName = AuthenticationFilter.class.getName() + "-role";
    public static final String AuthenticatedDataAttributeName = AuthenticationFilter.class.getName() + "-data";
    public static final String PULSAR_AUTH_METHOD_NAME = "X-Pulsar-Auth-Method-Name";


    public AuthenticationFilter(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request.getAttribute(AuthenticatedRoleAttributeName) != null) {
            chain.doFilter(request, response);
            return;
        }
        AsyncContext asyncContext = request.startAsync();
        asyncContext.addListener(new AsyncListener() {
            @Override
            public void onComplete(AsyncEvent event) throws IOException {
                try {
                    chain.doFilter(event.getSuppliedRequest(), event.getSuppliedResponse());
                } catch (ServletException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onTimeout(AsyncEvent event) throws IOException {

            }

            @Override
            public void onError(AsyncEvent event) throws IOException {

            }

            @Override
            public void onStartAsync(AsyncEvent event) throws IOException {

            }
        });
        authenticationService
                .authenticateHttpRequestAsync((HttpServletRequest) request, (HttpServletResponse) response)
                .whenComplete((doFilter, throwable) -> {
                    if (throwable != null) {
                        try {
                            HttpServletResponse httpResponse = (HttpServletResponse) asyncContext.getResponse();
                            httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Authentication required");
                            if (throwable instanceof AuthenticationException) {
                                LOG.warn("[{}] Failed to authenticate HTTP request: {}", request.getRemoteAddr(),
                                        throwable.getMessage());
                            } else {
                                LOG.error("[{}] Error performing authentication for HTTP", request.getRemoteAddr(),
                                        throwable);
                            }
                        } catch (IOException e) {
                            LOG.error("Error while responding to HTTP request", e);
                        } finally {
                            asyncContext.complete();
                        }
                    } else {
                        asyncContext.getRequest().setAttribute("do_filter", doFilter);
                        asyncContext.complete();
                    }
                });
    }

    private void runFilter(FilterChain chain, AsyncContext asyncContext) {
        try {
            chain.doFilter(asyncContext.getRequest(), asyncContext.getResponse());
        } catch (IOException | ServletException e) {
            LOG.error("Error in HTTP filtering", e);
        } finally {
            asyncContext.complete();
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
