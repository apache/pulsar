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

import com.google.common.util.concurrent.RateLimiter;

import io.prometheus.client.Counter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

public class RateLimitingFilter implements Filter {

    private final RateLimiter limiter;

    public RateLimitingFilter(double rateLimit) {
        limiter = RateLimiter.create(rateLimit);
    }

    private static final Counter httpRejectedRequests = Counter.build()
            .name("pulsar_broker_http_rejected_requests")
            .help("Counter of HTTP requests rejected by rate limiting")
            .register();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (limiter.tryAcquire()) {
            chain.doFilter(request, response);
        } else {
            httpRejectedRequests.inc();
            HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.sendError(429, "Too Many Requests");
        }
    }

    @Override
    public void destroy() {
    }
}
