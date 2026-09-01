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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.common.configuration.PulsarConfiguration;

/**
 * An {@link AdditionalServlet} written against the legacy {@code javax.servlet} API, the way existing
 * third-party plugins are, so that tests can exercise the adaptation to {@code jakarta.servlet} that the
 * broker and the proxy perform before registering such a servlet (PIP-472).
 *
 * <p>The response echoes back parts of the request the plugin observes through the javax API, so that a test
 * can tell an adapted request apart from an empty or mistranslated one.
 */
public class LegacyJavaxAdditionalServlet extends HttpServlet implements AdditionalServlet {

    public static final String QUERY_PARAM = "param";

    private final String basePath;

    public LegacyJavaxAdditionalServlet(String basePath) {
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
        return AdditionalServletType.JAVAX_SERVLET;
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
        byte[] body = (request.getParameter(QUERY_PARAM) + "|" + request.getRequestURI() + "|" + request.getMethod())
                .getBytes(StandardCharsets.UTF_8);
        response.setContentType("text/plain");
        response.setContentLength(body.length);
        ServletOutputStream outputStream = response.getOutputStream();
        outputStream.write(body);
        outputStream.flush();
    }
}
