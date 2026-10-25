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
import lombok.CustomLog;
import org.apache.pulsar.broker.ServiceConfiguration;

/**
 * Servlet filter that rejects HTTP requests using TRACE/TRACK methods.
 */
@CustomLog
public class DisableDebugHttpMethodFilter implements Filter {

    private final ServiceConfiguration serviceConfiguration;

    public DisableDebugHttpMethodFilter(ServiceConfiguration serviceConfiguration) {
        this.serviceConfiguration = serviceConfiguration;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        if (this.serviceConfiguration.isDisableHttpDebugMethods()) {
            if ("TRACE".equalsIgnoreCase(httpRequest.getMethod())) {
                // TRACE is not allowed
                httpResponse.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);

                log.info().attr("remoteAddr", request.getRemoteAddr()).log("Rejected HTTP request using TRACE Method");
                return;
            } else if ("TRACK".equalsIgnoreCase(httpRequest.getMethod())) {
                // TRACK is not allowed
                httpResponse.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);

                log.info().attr("remoteAddr", request.getRemoteAddr()).log("Rejected HTTP request using TRACK Method");
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
