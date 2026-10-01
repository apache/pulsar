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
package org.apache.pulsar.broker.web.plugin.servlet;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

/**
 * An {@link AdditionalServlet} written against the {@code jakarta.servlet} API, the counterpart of
 * {@link LegacyJavaxAdditionalServlet}. The two echo back the same parts of the request, so that a test can
 * assert the adapted legacy servlet observes exactly what a native one does.
 */
public class JakartaAdditionalServlet extends HttpServlet implements AdditionalServlet {

    private final String basePath;

    public JakartaAdditionalServlet(String basePath) {
        this.basePath = basePath;
    }

    /**
     * Builds the response body this servlet answers a {@code GET basePath?param=<paramValue>} with. The request
     * URI carries a trailing slash because Jetty redirects a bare context path to its canonical form.
     */
    public static String expectedResponse(String basePath, String paramValue) {
        return paramValue + "|" + basePath + "/|GET";
    }

    @Override
    public void loadConfig(PulsarConfiguration pulsarConfiguration) {
        // No config to load
    }

    @Override
    public String getBasePath() {
        return basePath;
    }

    @Override
    public AdditionalServletType getServletType() {
        return AdditionalServletType.JAKARTA_SERVLET;
    }

    @Override
    public Object getServletInstance() {
        return this;
    }

    @Override
    public void close() {
        // Nothing to close
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        byte[] body = (request.getParameter(LegacyJavaxAdditionalServlet.QUERY_PARAM) + "|"
                + request.getRequestURI() + "|" + request.getMethod()).getBytes(StandardCharsets.UTF_8);
        response.setContentType("text/plain");
        response.setContentLength(body.length);
        ServletOutputStream outputStream = response.getOutputStream();
        outputStream.write(body);
        outputStream.flush();
    }
}
