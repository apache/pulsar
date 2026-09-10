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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * An immutable buffered HTTP response (PIP-478).
 *
 * <p>The body is fully buffered into a byte array (streaming responses are out of scope for v1).
 * Header lookups via {@link #header(String)} are case-insensitive. The header map is single-valued per
 * canonical name: two source entries that canonicalise to the same name (case-variants) collapse, and the
 * one visited last in the source map's iteration order wins; multi-valued headers are not represented.
 *
 * <p><b>Body ownership: the array is handed off, not copied.</b> {@link #of} takes ownership of the
 * {@code body} array without copying it, and {@link #body()} returns that same array without copying;
 * the array must not be mutated after the response is constructed or after {@code body()} hands it out.
 */
public final class HttpResponse {

    private final int statusCode;
    private final Map<String, String> headers;
    private final byte[] body;

    private HttpResponse(int statusCode, Map<String, String> headers, byte[] body) {
        this.statusCode = statusCode;
        Map<String, String> copy = new LinkedHashMap<>();
        if (headers != null) {
            headers.forEach((k, v) -> copy.put(canonical(k), v));
        }
        this.headers = Collections.unmodifiableMap(copy);
        this.body = body == null ? new byte[0] : body;
    }

    /**
     * Create a response.
     *
     * @param statusCode the HTTP status code
     * @param headers    the response headers (names canonicalised; may be {@code null})
     * @param body       the response body bytes (may be {@code null}, treated as empty)
     * @return a new {@link HttpResponse}
     */
    public static HttpResponse of(int statusCode, Map<String, String> headers, byte[] body) {
        return new HttpResponse(statusCode, headers, body);
    }

    /**
     * @return the HTTP status code
     */
    public int statusCode() {
        return statusCode;
    }

    /**
     * @return an unmodifiable, canonical-cased view of the response headers
     */
    public Map<String, String> headers() {
        return headers;
    }

    /**
     * Look up a header value, case-insensitively.
     *
     * @param name the header name
     * @return the header value if present
     */
    public Optional<String> header(String name) {
        return Optional.ofNullable(headers.get(canonical(name)));
    }

    /**
     * @return the raw response body bytes (never {@code null})
     */
    public byte[] body() {
        return body;
    }

    /**
     * Decode the body as UTF-8, ignoring any charset declared in the response {@code Content-Type}.
     *
     * @return the response body decoded as UTF-8
     */
    public String bodyAsString() {
        return new String(body, StandardCharsets.UTF_8);
    }

    private static String canonical(String name) {
        // RFC 7230 header names are case-insensitive; normalise to lower case for lookups.
        return name == null ? null : name.toLowerCase(Locale.ROOT);
    }
}
