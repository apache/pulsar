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

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;

/**
 * Http request wrapper.
 *
 * <p>The http request wrapper is used to allow read http body multiple times.
 */
public class RequestWrapper extends HttpServletRequestWrapper {

    private byte[] body;
    private ServletInputStream inputStream;

    public RequestWrapper(HttpServletRequest request) throws IOException {
        super(request);
    }

    /**
     * Buffers the request body on first access. Buffering is lazy on purpose: an interceptor that
     * does not read the body (the common case) must not cause the body to be buffered at all, so the
     * underlying request stream is consumed only once, by the downstream resource (Jersey). The read
     * is bounded to Content-Length so it never reads past the request body.
     */
    private byte[] body() throws IOException {
        if (body == null) {
            HttpServletRequest request = (HttpServletRequest) getRequest();
            int contentLength = request.getContentLength();
            if (contentLength >= 0) {
                body = IOUtils.toByteArray(request.getInputStream(), contentLength);
            } else {
                body = IOUtils.toByteArray(request.getInputStream());
            }
        }
        return body;
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        if (inputStream == null) {
            // Return a single, stable stream over the buffered body. Repeated getInputStream() calls
            // must return the same stream (per the Servlet contract); returning a fresh stream each
            // time lets a reader that re-fetches the stream after EOF read the body's first byte
            // again, surfacing as a spurious "Trailing token" error.
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body());
            inputStream = new ServletInputStream() {
                @Override
                public boolean isFinished() {
                    return byteArrayInputStream.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener readListener) {

                }

                @Override
                public int read() {
                    return byteArrayInputStream.read();
                }
            };
        }
        return inputStream;
    }

    @Override
    public BufferedReader getReader() throws IOException {
        return new BufferedReader(new InputStreamReader(this.getInputStream(), Charset.defaultCharset()));
    }

    //Use this method to read the request body N times
    public byte[] getBody() throws IOException {
        return body();
    }
}
