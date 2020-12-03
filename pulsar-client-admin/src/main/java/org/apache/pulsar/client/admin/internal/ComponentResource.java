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
package org.apache.pulsar.client.admin.internal;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.sasl.SaslConstants;
import org.asynchttpclient.RequestBuilder;

/**
 * Abstract base class for component resources.
 */
public class ComponentResource extends BaseResource {

    protected ComponentResource(Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
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
        AuthenticationDataProvider authData = auth.getAuthData(target.getUri().getHost());
        String targetUrl = target.getUri().toString();
        if (auth.getAuthMethodName().equalsIgnoreCase(SaslConstants.AUTH_METHOD_NAME)) {
            CompletableFuture<Map<String, String>> authFuture = new CompletableFuture<>();
            auth.authenticationStage(targetUrl, authData, null, authFuture);
            return auth.newRequestHeader(targetUrl, authData, authFuture.get());
        } else if (authData.hasDataForHttp()) {
            return auth.newRequestHeader(targetUrl, authData, null);
        } else {
            return null;
        }
    }
}
