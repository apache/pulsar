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
import javax.ws.rs.core.Response;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConnectException;
import org.apache.pulsar.client.admin.PulsarAdminException.GettingAuthenticationDataException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAllowedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.admin.PulsarAdminException.ServerSideErrorException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for all admin resources.
 */
public abstract class BaseResource {
    private static final Logger log = LoggerFactory.getLogger(BaseResource.class);

    protected final Authentication auth;
    protected final long readTimeoutMs;

    protected BaseResource(Authentication auth, long readTimeoutMs) {
        this.auth = auth;
        this.readTimeoutMs = readTimeoutMs;
    }

    public Builder request(final WebTarget target) throws PulsarAdminException {
        try {
            return requestAsync(target).get();
        } catch (Exception e) {
            throw new GettingAuthenticationDataException(e);
        }
    }

    // do the authentication stage, and once authentication completed return a Builder
    public CompletableFuture<Builder> requestAsync(final WebTarget target) {
        CompletableFuture<Builder> builderFuture = new CompletableFuture<>();
        CompletableFuture<Map<String, String>> authFuture = new CompletableFuture<>();
        try {
            AuthenticationDataProvider authData = auth.getAuthData(target.getUri().getHost());

            if (authData.hasDataForHttp()) {
                auth.authenticationStage(target.getUri().toString(), authData, null, authFuture);
            } else {
                authFuture.complete(null);
            }

            // auth complete, return a new Builder
            authFuture.whenComplete((respHeaders, ex) -> {
                if (ex != null) {
                    log.warn("[{}] Failed to perform http request at auth stage: {}", target.getUri(),
                        ex.getMessage());
                    builderFuture.completeExceptionally(new PulsarClientException(ex));
                    return;
                }

                try {
                    Builder builder = target.request(MediaType.APPLICATION_JSON);
                    if (authData.hasDataForHttp()) {
                        Set<Entry<String, String>> headers =
                            auth.newRequestHeader(target.getUri().toString(), authData, respHeaders);
                        if (headers != null) {
                            headers.forEach(entry -> builder.header(entry.getKey(), entry.getValue()));
                        }
                    }
                    builderFuture.complete(builder);
                } catch (Throwable t) {
                    builderFuture.completeExceptionally(new GettingAuthenticationDataException(t));
                }
            });
        } catch (Throwable t) {
            builderFuture.completeExceptionally(new GettingAuthenticationDataException(t));
        }

        return builderFuture;
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
        if (e instanceof PulsarAdminException) {
            return (PulsarAdminException) e;
        } else if (e instanceof ServiceUnavailableException) {
            if (e.getCause() instanceof java.net.ConnectException) {
                return new ConnectException(e.getCause());
            } else {
                return new PulsarAdminException((ServerErrorException) e);
            }
        } else if (e instanceof WebApplicationException) {
            // Handle 5xx exceptions
            if (e instanceof ServerErrorException) {
                ServerErrorException see = (ServerErrorException) e;
                return new ServerSideErrorException(see, e.getMessage());
            } else if (e instanceof ClientErrorException) {
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
                return new PulsarAdminException((WebApplicationException) e);
            }
        } else {
            return new PulsarAdminException(e);
        }
    }

    public PulsarAdminException getApiException(Response response) {
        if (response.getStatusInfo().equals(Response.Status.OK)) {
            return null;
        }
        try {
            if (response.getStatus() >= 500) {
                throw new ServerErrorException(response);
            } else if (response.getStatus() >= 400) {
                throw new ClientErrorException(response);
            } else {
                throw new WebApplicationException(response);
            }
        } catch (Exception e) {
            return getApiException(e);
        }
    }
}
