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

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import org.eclipse.jetty.servlets.CrossOriginFilter;

/**
 * This class separates the CORS handling into an own place.
 * However, it requires quite some code and inheritance, so I think it is harder to reason about.
 * Personally I would go with the approach shown in WebService.class
 */

public class CorsFilter extends CrossOriginFilter {

    private final String allowedOrigins;

    public CorsFilter(String allowedOrigins) {
        this.allowedOrigins = allowedOrigins;
    }

    @Override
    public void init(FilterConfig arg) throws ServletException {
        super.init(new CorsFilterConfig(allowedOrigins, arg));
    }

    public static final class CorsFilterConfig implements FilterConfig {

        private final String filterName;
        private final ServletContext servletContext;
        private final Map<String, String> map = new HashMap<>();

        public CorsFilterConfig(String allowedOrigins, FilterConfig filterConfig) {
            this.filterName = filterConfig.getFilterName();
            this.servletContext = filterConfig.getServletContext();

            map.put(ALLOWED_ORIGINS_PARAM, allowedOrigins);
            map.put(ALLOWED_METHODS_PARAM, "POST,GET,OPTIONS,PUT,DELETE,HEAD");
            map.put(ALLOWED_HEADERS_PARAM, "Origin, X-Requested-With, Content-Type, Accept");
            map.put(PREFLIGHT_MAX_AGE_PARAM, "86400"); // 24 hours
            map.put(ALLOW_CREDENTIALS_PARAM, "true");
        }

        @Override
        public String getFilterName() {
            return filterName;
        }

        @Override
        public ServletContext getServletContext() {
            return servletContext;
        }

        @Override
        public String getInitParameter(String s) {
            return map.get(s);
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            return Collections.enumeration(this.map.keySet());
        }
    }
}
