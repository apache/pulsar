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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConnectException;
import org.apache.pulsar.client.admin.PulsarAdminException.GettingAuthenticationDataException;
import org.apache.pulsar.client.admin.PulsarAdminException.HttpErrorException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAllowedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.admin.PulsarAdminException.ServerSideErrorException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.util.FutureUtil;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseResource {
    private static final Logger log = LoggerFactory.getLogger(BaseResource.class);

    private final Authentication auth;

    protected BaseResource(Authentication auth) {
        this.auth = auth;
    }

    public Builder request(final WebTarget target) throws PulsarAdminException {
        try {
            Builder builder = target.request(MediaType.APPLICATION_JSON);
            // Add headers for authentication if any
            if (auth != null && auth.getAuthData().hasDataForHttp()) {
                for (Map.Entry<String, String> header : auth.getAuthData().getHttpHeaders()) {
                    builder.header(header.getKey(), header.getValue());
                }
            }
            return builder;
        } catch (Throwable t) {
            throw new GettingAuthenticationDataException(t);
        }
    }

    public <T> CompletableFuture<Void> asyncPutRequest(final WebTarget target, Entity<T> entity) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            request(target).async().put(entity, new InvocationCallback<ErrorData>() {

                @Override
                public void completed(ErrorData response) {
                    future.complete(null);
                }

                @Override
                public void failed(Throwable throwable) {
                    log.warn("[{}] Failed to perform http put request: {}", target.getUri(), throwable.getMessage());
                    future.completeExceptionally(getApiException(throwable.getCause()));
                }

            });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    public <T> CompletableFuture<Void> asyncPostRequest(final WebTarget target, Entity<T> entity) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            request(target).async().post(entity, new InvocationCallback<ErrorData>() {

                @Override
                public void completed(ErrorData response) {
                    future.complete(null);
                }

                @Override
                public void failed(Throwable throwable) {
                    log.warn("[{}] Failed to perform http post request: {}", target.getUri(), throwable.getMessage());
                    future.completeExceptionally(getApiException(throwable.getCause()));
                }

            });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    public <T> Future<T> asyncGetRequest(final WebTarget target, InvocationCallback<T> callback) {
        try {
            return request(target).async().get(callback);
        } catch (PulsarAdminException cae) {
            return FutureUtil.failedFuture(cae);
        }
    }

    public CompletableFuture<Void> asyncDeleteRequest(final WebTarget target) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            request(target).async().delete(new InvocationCallback<ErrorData>() {

                @Override
                public void completed(ErrorData response) {
                    future.complete(null);
                }

                @Override
                public void failed(Throwable throwable) {
                    log.warn("[{}] Failed to perform http delete request: {}", target.getUri(), throwable.getMessage());
                    future.completeExceptionally(getApiException(throwable.getCause()));
                }
            });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    public PulsarAdminException getApiException(Throwable e) {
        if (e instanceof ServiceUnavailableException) {
            if (e.getCause() instanceof java.net.ConnectException) {
                return new ConnectException(e.getCause());
            } else {
                return new HttpErrorException(e);
            }
        } else if (e instanceof WebApplicationException) {
            // Handle 5xx exceptions
            if (e instanceof ServerErrorException) {
                ServerErrorException see = (ServerErrorException) e;
                return new ServerSideErrorException(see);
            }

            // Handle 4xx exceptions
            ClientErrorException cee = (ClientErrorException) e;
            int statusCode = cee.getResponse().getStatus();
            switch (statusCode) {
            case 401:
            case 403:
                return new NotAuthorizedException(cee);
            case 404:
                return new NotFoundException(cee);
            case 405:
                return new NotAllowedException(cee);
            case 409:
                return new ConflictException(cee);
            case 412:
                return new PreconditionFailedException(cee);

            default:
                return new PulsarAdminException(cee);
            }
        } else {
            return new PulsarAdminException(e);
        }
    }

}
