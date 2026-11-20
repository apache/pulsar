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

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.function.Predicate;
import javax.naming.AuthenticationException;
import lombok.CustomLog;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Servlet filter that hooks up with AuthenticationService to reject unauthenticated HTTP requests.
 */
@CustomLog
public class AuthenticationFilter implements Filter {

    private static final String AUTHENTICATION_REQUIRED = "Authentication required";
    private final AuthenticationService authenticationService;
    private final Predicate<HttpServletRequest> useJsonErrors;

    public static final String AuthenticatedRoleAttributeName = AuthenticationFilter.class.getName() + "-role";
    public static final String AuthenticatedDataAttributeName = AuthenticationFilter.class.getName() + "-data";
    public static final String PULSAR_AUTH_METHOD_NAME = "X-Pulsar-Auth-Method-Name";


    public AuthenticationFilter(AuthenticationService authenticationService) {
        this(authenticationService, request -> false);
    }

    public AuthenticationFilter(AuthenticationService authenticationService,
                                Predicate<HttpServletRequest> useJsonErrors) {
        this.authenticationService = authenticationService;
        this.useJsonErrors = useJsonErrors;
    }

    @Override
    public void doFilter(
            ServletRequest request, ServletResponse response, FilterChain chain
    ) throws IOException, ServletException {
        final HttpServletRequest httpRequest = (HttpServletRequest) request;
        final HttpServletResponse httpResponse = (HttpServletResponse) response;

        final boolean doFilter;
        try {
            doFilter = authenticationService.authenticateHttpRequest(httpRequest, httpResponse);
        } catch (Exception e) {
            if (e instanceof AuthenticationException) {
                String msg = e.getMessage();
                if (msg == null) {
                    msg = AUTHENTICATION_REQUIRED;
                }
                sendUnauthorized(httpRequest, httpResponse, msg);
                log.warn()
                        .attr("remoteAddr", request.getRemoteAddr())
                        .attr("request", msg)
                        .log("Failed to authenticate HTTP request");
            } else {
                sendUnauthorized(httpRequest, httpResponse, AUTHENTICATION_REQUIRED);
                log.error()
                        .attr("remoteAddr", request.getRemoteAddr())
                        .exception(e)
                        .log("Error performing authentication for HTTP");
            }
            return;
        }

        if (doFilter) {
            chain.doFilter(request, response);
        }
    }

    private void sendUnauthorized(HttpServletRequest request, HttpServletResponse response, String message)
            throws IOException {
        if (!useJsonErrors.test(request)) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, message);
            return;
        }

        byte[] body = ObjectMapperFactory.getMapper().writer()
                .writeValueAsBytes(new ErrorData(AUTHENTICATION_REQUIRED));
        response.resetBuffer();
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json;charset=utf-8");
        response.setContentLength(body.length);
        response.getOutputStream().write(body);
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
