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
package org.apache.pulsar.http;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * An immutable HTTP request (PIP-478).
 *
 * <p>Construct via {@link #builder(Method, URI)}. The optional {@link Body} is a sealed type so the
 * set of body shapes (currently only {@link Bytes}) is closed; the framework's single body-encoding
 * site dispatches on the body type.
 *
 * <p><b>Headers are single-valued and keyed by exact name.</b> Unlike {@link HttpResponse} (whose names
 * are canonicalised), request header names are NOT case-folded: they are stored verbatim as supplied to
 * the builder. Setting a header whose name exactly matches an existing one replaces the prior value
 * (last-wins); names that differ only in case are therefore distinct entries, and multi-valued headers are
 * not representable.
 */
public final class HttpRequest {

    /** The HTTP method. */
    public enum Method {
        GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH
    }

    /** A request body. Sealed to the single {@link Bytes} shape. */
    public sealed interface Body permits Bytes {
    }

    /**
     * A raw byte body with an explicit content type.
     *
     * <p><b>Ownership: the array is handed off, not copied.</b> The caller transfers ownership of
     * {@code content} to this body on construction and the framework reads it without copying; neither
     * side may mutate the array after construction. Because the component is an array, the record's
     * generated {@code equals}/{@code hashCode} are reference-based, not value-based; do not compare
     * {@link Bytes} instances for value equality.
     *
     * @param content     the body bytes
     * @param contentType the {@code Content-Type} value
     */
    public record Bytes(byte[] content, String contentType) implements Body {
    }

    private final Method method;
    private final URI uri;
    private final Map<String, String> headers;
    private final Body body;

    private HttpRequest(Builder b) {
        this.method = b.method;
        this.uri = b.uri;
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(b.headers));
        this.body = b.body;
    }

    /**
     * @return the HTTP method
     */
    public Method method() {
        return method;
    }

    /**
     * @return the request URI
     */
    public URI uri() {
        return uri;
    }

    /**
     * @return an unmodifiable view of the request headers
     */
    public Map<String, String> headers() {
        return headers;
    }

    /**
     * @return the request body, if any
     */
    public Optional<Body> body() {
        return Optional.ofNullable(body);
    }

    /**
     * Create a builder.
     *
     * @param method the HTTP method
     * @param uri    the request URI
     * @return a new {@link Builder}
     */
    public static Builder builder(Method method, URI uri) {
        return new Builder(method, uri);
    }

    /**
     * Builder for {@link HttpRequest}.
     */
    public static final class Builder {
        private final Method method;
        private final URI uri;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private Body body;

        private Builder(Method method, URI uri) {
            this.method = method;
            this.uri = uri;
        }

        /**
         * Add or replace a header.
         *
         * @param name  the header name
         * @param value the header value
         * @return this builder
         */
        public Builder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        /**
         * Set the request body.
         *
         * @param body the body
         * @return this builder
         */
        public Builder body(Body body) {
            this.body = body;
            return this;
        }

        /**
         * @return a new immutable {@link HttpRequest}
         */
        public HttpRequest build() {
            return new HttpRequest(this);
        }
    }
}
