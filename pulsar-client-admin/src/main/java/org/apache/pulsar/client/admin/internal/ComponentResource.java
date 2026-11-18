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
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.asynchttpclient.RequestBuilder;

/**
 * Abstract base class for component resources.
 */
public class ComponentResource extends BaseResource {

    protected ComponentResource(Authentication auth, long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
    }

    /**
     * Attach the authentication headers for {@code target} to {@code requestBuilder}, completing once they
     * are known.
     *
     * <p>Composing them asynchronously keeps the operations that submit over the raw AsyncHttpClient off the
     * blocking path the v4 hooks used to take here. It matters since the v5 SASL warmup goes over the same
     * connector those operations submit over (PIP-478): waiting for it on the calling thread deadlocks a
     * caller that chains an admin operation onto a previous admin future, because that continuation runs on
     * the connector's IO thread and the warmup needs the very loop it would hold.
     */
    public CompletableFuture<RequestBuilder> addAuthHeaders(WebTarget target, RequestBuilder requestBuilder) {
        if (auth == null) {
            return CompletableFuture.completedFuture(requestBuilder);
        }
        CompletableFuture<RequestBuilder> future = new CompletableFuture<>();
        try {
            computeAuthHeaders(target.getUri()).whenComplete((headers, ex) -> {
                if (ex != null) {
                    future.completeExceptionally(new PulsarAdminException.GettingAuthenticationDataException(
                            FutureUtil.unwrapCompletionException(ex)));
                    return;
                }
                try {
                    if (headers != null) {
                        headers.forEach(requestBuilder::addHeader);
                    }
                    future.complete(requestBuilder);
                } catch (Throwable t) {
                    future.completeExceptionally(new PulsarAdminException.GettingAuthenticationDataException(t));
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(new PulsarAdminException.GettingAuthenticationDataException(t));
        }
        return future;
    }

    protected ObjectWriter objectWriter() {
        return ObjectMapperFactory.getMapper().writer();
    }

    protected ObjectReader objectReader() {
        return ObjectMapperFactory.getMapper().reader();
    }
}
