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
package org.apache.pulsar.client.api;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * HTTP request used by authentication providers.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public final class AuthenticationHttpRequest {
    private final Method method;
    private final String url;
    private final Map<String, String> headers;
    private final byte[] body;

    private AuthenticationHttpRequest(Builder builder) {
        this.method = builder.method;
        this.url = builder.url;
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(builder.headers));
        this.body = builder.body == null ? null : builder.body.clone();
    }

    public Method getMethod() {
        return method;
    }

    public String getUrl() {
        return url;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body == null ? null : body.clone();
    }

    public static Builder get(String url) {
        return new Builder(Method.GET, url);
    }

    public static Builder post(String url) {
        return new Builder(Method.POST, url);
    }

    public enum Method {
        GET,
        POST
    }

    /**
     * Builder for {@link AuthenticationHttpRequest}.
     */
    public static final class Builder {
        private final Method method;
        private final String url;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private byte[] body;

        private Builder(Method method, String url) {
            this.method = method;
            this.url = url;
        }

        public Builder header(String name, String value) {
            this.headers.put(name, value);
            return this;
        }

        public Builder body(byte[] body) {
            this.body = body == null ? null : body.clone();
            return this;
        }

        public Builder body(String body) {
            this.body = body == null ? null : body.getBytes(StandardCharsets.UTF_8);
            return this;
        }

        public AuthenticationHttpRequest build() {
            return new AuthenticationHttpRequest(this);
        }
    }
}
