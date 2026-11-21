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

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.sasl.SaslConstants;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.eclipse.jetty.ee10.servlet.ServletContextResponse;
import org.eclipse.jetty.http.HttpStatus;

/** Writes the subscription cursor stats error format when a servlet filter rejects a request before Jersey. */
final class SubscriptionCursorStatsErrorFilter extends HttpFilter {
    private static final Pattern MATRIX_PARAMS = Pattern.compile(";[^/]*");
    private static final Pattern PATH = Pattern.compile(
            "/admin/v2/persistent/[^/]+/[^/]+/[^/]+/subscription/[^/]+/(?:partitioned-)?internalStats/?");

    @Override
    protected void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        // Jersey ignores matrix parameters when matching routes. Keep encoded semicolons and slashes in names.
        String path = request.getRequestURI();
        if (path.indexOf(';') >= 0) {
            path = MATRIX_PARAMS.matcher(path).replaceAll("");
        }
        if (!"GET".equals(request.getMethod()) || !PATH.matcher(path).matches()) {
            chain.doFilter(request, response);
            return;
        }

        JsonErrorResponse wrapped = new JsonErrorResponse(request, response);
        chain.doFilter(request, wrapped);
        // A provider may finish authentication with setStatus instead of sendError.
        if (!request.isAsyncStarted() && !wrapped.isCommitted() && wrapped.getStatus() >= 400
                && (wrapped.getContentType() == null
                        || !wrapped.getContentType().startsWith(MediaType.APPLICATION_JSON))
                && !wrapped.isSaslHandshake()) {
            wrapped.sendError(wrapped.getStatus());
        }
    }

    private static final class JsonErrorResponse extends HttpServletResponseWrapper {
        private final HttpServletRequest request;

        private JsonErrorResponse(HttpServletRequest request, HttpServletResponse response) {
            super(response);
            this.request = request;
        }

        @Override
        public void sendError(int status) throws IOException {
            sendError(status, null);
        }

        @Override
        public void sendError(int status, String message) throws IOException {
            if (status < 400 || (status == SC_UNAUTHORIZED && isSaslHandshake())) {
                super.sendError(status, message);
                return;
            }
            String reason = status == SC_UNAUTHORIZED ? "Authentication required" : HttpStatus.getMessage(status);
            // Release any writer selected by the provider while preserving authentication headers.
            ServletContextResponse.getServletContextResponse(this).resetContent();
            setStatus(status);
            setContentType(MediaType.APPLICATION_JSON);
            ObjectMapperFactory.getMapper().writer().writeValue(getOutputStream(), new ErrorData(reason));
        }

        private boolean isSaslHandshake() {
            // Challenges and cached-token checks use the same URI as the eventual resource request.
            if (!SaslConstants.SASL_TYPE_VALUE.equalsIgnoreCase(getHeader(SaslConstants.SASL_HEADER_TYPE))) {
                return false;
            }
            return SaslConstants.SASL_STATE_NEGOTIATE.equalsIgnoreCase(getHeader(SaslConstants.SASL_HEADER_STATE))
                    || SaslConstants.SASL_STATE_SERVER_CHECK_TOKEN.equalsIgnoreCase(
                            request.getHeader(SaslConstants.SASL_HEADER_STATE));
        }
    }
}
