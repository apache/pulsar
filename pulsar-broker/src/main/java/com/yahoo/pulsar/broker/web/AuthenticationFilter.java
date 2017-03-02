/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.web;

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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.authentication.AuthenticationService;

/**
 * Servlet filter that hooks up with AuthenticationService to reject unauthenticated HTTP requests
 */
public class AuthenticationFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationFilter.class);

    private final AuthenticationService authenticationService;

    public static final String AuthenticatedRoleAttributeName = AuthenticationFilter.class.getName() + "-role";

    public AuthenticationFilter(PulsarService pulsar) {
        this.authenticationService = pulsar.getBrokerService().getAuthenticationService();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        try {
            String role = authenticationService.authenticateHttpRequest((HttpServletRequest) request);
            request.setAttribute(AuthenticatedRoleAttributeName, role);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Authenticated HTTP request with role {}", request.getRemoteAddr(), role);
            }
        } catch (AuthenticationException e) {
            HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Authentication required");
            LOG.warn("[{}] Failed to authenticate HTTP request: {}", request.getRemoteAddr(), e.getMessage());
            return;
        }

        try {
            chain.doFilter(request, response);
        } catch (WebApplicationException we) {
            if (we.getResponse().getStatus() == Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
                // kill the current session
                try {
                    ((HttpServletRequest) request).getSession(false).invalidate();
                } catch (Exception ignoreException) {
                    /* connection is already invalidated */
                }
            }
            throw we;
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
