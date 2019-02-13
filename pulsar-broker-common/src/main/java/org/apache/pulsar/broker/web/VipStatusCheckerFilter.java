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
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet filter that only allows requests for vip url.
 */
public class VipStatusCheckerFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(VipStatusCheckerFilter.class);

    private final String statusPath;
    private final int statusPort;

    public VipStatusCheckerFilter(String vipPath, int port) {
        this.statusPath = vipPath;
        this.statusPort = port;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        if (request instanceof HttpServletRequest) {
            HttpServletRequest httpRequest = ((HttpServletRequest) request);
            String path = httpRequest.getPathInfo();
            int port = httpRequest.getServerPort();
            // only status-health check url call is allowed to call on this port
            if (statusPort == port && !statusPath.equalsIgnoreCase(path)) {
                HttpServletResponse httpResponse = (HttpServletResponse) response;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} is not allowed on port {}", httpRequest.getRequestURL(), statusPort);
                }
                httpResponse.sendError(HttpServletResponse.SC_NOT_FOUND, "Request url not found");
                return;
            }
        }

        chain.doFilter(request, response);
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
