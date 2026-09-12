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
package org.apache.pulsar.client.api.v5.auth;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * HTTP authentication headers an implementation produces for an outgoing request (and, for
 * challenge/response, the headers carrying a server's challenge) (PIP-478).
 *
 * <p>Header names are canonicalised on construction (RFC 7230 §3.2 treats them case-insensitively);
 * {@link #get(String)} is case-insensitive; {@link #asMap()} returns a stable canonical-cased view.
 *
 * <p><b>Single value per name.</b> This collapses to one value per canonical name: when the source map
 * passed to {@link #of(Map)} holds two entries that canonicalise to the same name (case-variants such as
 * {@code Authorization} and {@code authorization}), the one visited last in the source map's iteration
 * order wins. Multi-valued headers are not represented.
 */
public final class HttpAuthHeaders {

    private static final HttpAuthHeaders EMPTY = new HttpAuthHeaders(Collections.emptyMap());

    private final Map<String, String> headers;

    private HttpAuthHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    /**
     * @return a shared empty instance
     */
    public static HttpAuthHeaders empty() {
        return EMPTY;
    }

    /**
     * Create headers with a single entry.
     *
     * @param name  the header name
     * @param value the header value
     * @return a new {@link HttpAuthHeaders}
     */
    public static HttpAuthHeaders of(String name, String value) {
        Map<String, String> map = new LinkedHashMap<>(1);
        map.put(canonical(name), value);
        return new HttpAuthHeaders(Collections.unmodifiableMap(map));
    }

    /**
     * Create headers from a map. Names are canonicalised; two source entries that canonicalise to the
     * same name (case-variants) collapse to one, and the entry visited last in {@code headers}' iteration
     * order wins.
     *
     * @param headers the source headers (names canonicalised)
     * @return a new {@link HttpAuthHeaders}
     */
    public static HttpAuthHeaders of(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return EMPTY;
        }
        Map<String, String> map = new LinkedHashMap<>(headers.size());
        headers.forEach((k, v) -> map.put(canonical(k), v));
        return new HttpAuthHeaders(Collections.unmodifiableMap(map));
    }

    /**
     * Look up a header value, case-insensitively.
     *
     * @param name the header name
     * @return the header value if present
     */
    public Optional<String> get(String name) {
        return Optional.ofNullable(headers.get(canonical(name)));
    }

    /**
     * @return an unmodifiable, canonical-cased view of the headers
     */
    public Map<String, String> asMap() {
        return headers;
    }

    /**
     * @return {@code true} if there are no headers
     */
    public boolean isEmpty() {
        return headers.isEmpty();
    }

    private static String canonical(String name) {
        return name == null ? null : name.toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return "HttpAuthHeaders" + headers.keySet();
    }
}
