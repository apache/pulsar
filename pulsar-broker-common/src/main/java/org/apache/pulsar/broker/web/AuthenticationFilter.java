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
import javax.naming.AuthenticationException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.common.sasl.SaslConstants;
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

    public AuthenticationFilter(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    private boolean isSaslRequest(HttpServletRequest request) {
        if (request.getHeader(SaslConstants.SASL_HEADER_TYPE) == null
                || request.getHeader(SaslConstants.SASL_HEADER_TYPE).isEmpty()) {
            return false;
        }
        if (request.getHeader(SaslConstants.SASL_HEADER_TYPE)
            .equalsIgnoreCase(SaslConstants.SASL_TYPE_VALUE)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        try {
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            HttpServletResponse httpResponse = (HttpServletResponse) response;

            if (!isSaslRequest(httpRequest)) {
                // not sasl type, return role directly.
                String role = authenticationService.authenticateHttpRequest((HttpServletRequest) request);
                request.setAttribute(AuthenticatedRoleAttributeName, role);
                request.setAttribute(AuthenticatedDataAttributeName,
                    new AuthenticationDataHttps((HttpServletRequest) request));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Authenticated HTTP request with role {}", request.getRemoteAddr(), role);
                }
                chain.doFilter(request, response);
                return;
            }

            boolean doFilter = authenticationService
                .getAuthenticationProvider(SaslConstants.AUTH_METHOD_NAME)
                .authenticateHttpRequest(httpRequest, httpResponse);

            if (doFilter) {
                chain.doFilter(request, response);
            }
        } catch (Exception e) {
            HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Authentication required");
            if (e instanceof AuthenticationException) {
                LOG.warn("[{}] Failed to authenticate HTTP request: {}", request.getRemoteAddr(), e.getMessage());
            } else {
                LOG.error("[{}] Error performing authentication for HTTP", request.getRemoteAddr(), e);
            }
            return;
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
