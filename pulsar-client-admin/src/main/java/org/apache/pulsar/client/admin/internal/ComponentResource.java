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
package org.apache.pulsar.client.admin.internal;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import jakarta.ws.rs.client.WebTarget;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.asynchttpclient.RequestBuilder;

/**
 * Abstract base class for component resources.
 */
public class ComponentResource extends BaseResource {

    protected ComponentResource(Authentication auth, long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
    }

    public RequestBuilder addAuthHeaders(WebTarget target, RequestBuilder requestBuilder) throws PulsarAdminException {

        try {
            if (auth != null) {
                Set<Entry<String, String>> headers = getAuthHeaders(target);
                if (headers != null && !headers.isEmpty()) {
                    headers.forEach(header -> requestBuilder.addHeader(header.getKey(), header.getValue()));
                }
            }
            return requestBuilder;
        } catch (Throwable t) {
            throw new PulsarAdminException.GettingAuthenticationDataException(t);
        }
    }

    private Set<Entry<String, String>> getAuthHeaders(WebTarget target) throws Exception {
        // The blocking get() runs on the thread that called the admin operation: addAuthHeaders is invoked
        // synchronously from the raw-AsyncHttpClient operations below, before any future of theirs exists.
        // That matters more than it used to, because a v5 SASL warmup now goes over the same connector
        // these operations submit over: a caller that chains one of them onto a previous admin future runs
        // here on that connector's IO thread, where waiting holds up the loop the warmup needs. It cannot
        // hang there — the driver bounds the whole exchange off the loop (CompletableFuture#orTimeout), so
        // the wait ends in an AuthenticationException at requestTimeoutMs — but it is the reason not to
        // call these operations from an admin callback. This mirrors the blocking authFuture.get() it
        // replaces, now routed through the shared HTTP auth driver (PIP-478) when the plugin exposes the
        // v5 SASL-over-HTTP capability.
        Map<String, String> headers = computeAuthHeaders(target.getUri()).get();
        return (headers == null || headers.isEmpty()) ? null : headers.entrySet();
    }

    protected ObjectWriter objectWriter() {
        return ObjectMapperFactory.getMapper().writer();
    }

    protected ObjectReader objectReader() {
        return ObjectMapperFactory.getMapper().reader();
    }
}
