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

import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation.Builder;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.CustomLog;
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
import org.apache.pulsar.client.impl.auth.v5.AsyncHttpAuthenticationProvider;
import org.apache.pulsar.client.impl.auth.v5.HttpAuthenticationDriver;
import org.apache.pulsar.client.impl.auth.v5.V5AuthContexts;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Abstract base class for all admin resources.
 */
@CustomLog
public abstract class BaseResource {

    protected final Authentication auth;
    protected final long requestTimeoutMs;
    // PIP-478: the owning admin's bounded blocking executor, where the deprecated v4 credential composition
    // runs. Lent by PulsarAdminImpl right after construction rather than threaded through the constructors
    // of all 23 resource classes, and never mutated afterwards. Null for a resource built outside a
    // PulsarAdmin (tests, embedders), which has no admin pool to borrow and falls back to the framework's
    // shared one — still off the caller thread.
    private volatile Executor blockingAuthExecutor;

    protected BaseResource(Authentication auth, long requestTimeoutMs) {
        this.auth = auth;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    /**
     * Lend this resource the owning admin's bounded blocking authentication executor (PIP-478), so a
     * stalled identity provider reached through this admin's plugin cannot occupy the process-wide shared
     * pool that every other client in the JVM depends on. Called once by {@link PulsarAdminImpl}, before
     * the resource is published.
     *
     * @param blockingAuthExecutor the owning admin's blocking authentication executor
     */
    void setBlockingAuthExecutor(Executor blockingAuthExecutor) {
        this.blockingAuthExecutor = blockingAuthExecutor;
    }

    /**
     * The executor lent by {@link PulsarAdminImpl}, or {@code null} when none was (VisibleForTesting). The
     * lending is opt-in per construction site, and a resource that misses it still works — it just falls
     * back to the shared pool — so the only thing that can catch a resource added without it is a test that
     * reads this.
     *
     * @return the lent blocking authentication executor, or {@code null}
     */
    Executor blockingAuthExecutorForTest() {
        return blockingAuthExecutor;
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
        try {
            computeAuthHeaders(target.getUri()).whenComplete((authHeaders, ex) -> {
                if (ex != null) {
                    log.warn().attr("uri", target.getUri())
                            .exceptionMessage(ex)
                            .log("Failed to perform http request at auth stage");
                    builderFuture.completeExceptionally(new PulsarClientException(unwrapCompletion(ex)));
                    return;
                }

                try {
                    Builder builder = target.request(MediaType.APPLICATION_JSON);
                    if (authHeaders != null) {
                        authHeaders.forEach((name, value) -> builder.header(name, value));
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

    /**
     * Compute the authentication headers to attach to the outgoing admin request, or a future of
     * {@code null} when the plugin contributes none (PIP-478).
     *
     * <p>When the plugin exposes the v5-native SASL-over-HTTP capability (via the
     * {@link AsyncHttpAuthenticationProvider} bridge), the framework {@link HttpAuthenticationDriver} runs
     * the bounded {@code 401}→resubmit→{@code 200} exchange over the plugin's own HTTP client — a bodiless
     * {@code GET} to the original URI each round, exactly what the v4 {@code authenticationStage(...)} does
     * today — and yields the validated role-token headers. Otherwise the deprecated v4
     * {@code authenticationStage(...)} / {@code newRequestHeader(...)} hooks run verbatim, preserving
     * behaviour for third-party plugins and single-pass built-ins — but off the calling thread.
     */
    protected CompletableFuture<Map<String, String>> computeAuthHeaders(URI uri) {
        try {
            if (auth instanceof AsyncHttpAuthenticationProvider provider) {
                Optional<HttpAuthenticationDriver> driver = provider.httpAuthenticationDriver()
                        .filter(HttpAuthenticationDriver::supportsHttpChallenge);
                if (driver.isPresent()) {
                    Duration budget = Duration.ofMillis(requestTimeoutMs > 0 ? requestTimeoutMs : 60_000L);
                    // Null transport => the driver uses the plugin's own default transport (its JAX-RS client).
                    return driver.get().authenticateAsync(uri, null, budget)
                            .thenApply(headers -> (headers == null || headers.isEmpty()) ? null : headers.asMap());
                }
            }
            // The deprecated v4 hooks may block — an OAuth2 or Athenz shim's getAuthData() refreshes its
            // credential with a synchronous HTTP exchange — and this method runs on whatever thread issued the
            // admin call, which for a broker calling its own admin client is a request-handling thread.
            // HttpClient.computeAuthHeaders off-loads the identical composition for the lookup path and names
            // the self-deadlock it avoids; this was the last caller-thread credential resolution left, and the
            // one place PIP-478's "every synchronous v4 plugin call is off-loaded" was still an overstatement.
            //
            // It runs on the owning admin's own bounded pool, the same one a services-aware plugin is lent, so
            // the work that can actually stall — a KDC or IdP round trip inside the plugin — stays isolated
            // per admin: were this the framework's process-wide shared pool, one admin's stalled provider
            // would throttle authentication for every other client in the JVM. A resource built outside a
            // PulsarAdmin has no pool to borrow and falls back to that shared one, which is still off the
            // caller thread.
            Executor executor = V5AuthContexts.blockingExecutorOrShared(blockingAuthExecutor);
            return V5AuthContexts.supplyBlocking(executor, () -> v4AuthHeaders(uri, executor))
                    .thenCompose(headers -> headers);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    /**
     * The deprecated v4 HTTP authentication composition, run on a blocking executor by
     * {@link #computeAuthHeaders(URI)}.
     *
     * @param uri the request URI
     * @param blockingExecutor the executor this composition runs on, for the continuation below
     * @return a future of the headers, or of {@code null} when the plugin contributes none
     */
    private CompletableFuture<Map<String, String>> v4AuthHeaders(URI uri, Executor blockingExecutor) {
        try {
            AuthenticationDataProvider authData = auth.getAuthData(uri.getHost());
            if (!authData.hasDataForHttp()) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Map<String, String>> stage = new CompletableFuture<>();
            auth.authenticationStage(uri.toString(), authData, null, stage);
            // thenApplyAsync, not thenApply: newRequestHeader is the second synchronous v4 hook, and a plugin
            // that completes the stage asynchronously — the multi-round challenge shape — completes it from
            // its own HTTP callback thread. A plain continuation would run that hook there, which off-loading
            // the resolution alone would not cover. In-tree that path now belongs to the v5 driver above, so
            // this is for third-party v4 plugins; where the stage completes inline (the single-pass default)
            // the hop is to a sibling task on this same executor.
            return stage.thenApplyAsync(respHeaders -> {
                try {
                    Set<Entry<String, String>> headers = auth.newRequestHeader(uri.toString(), authData, respHeaders);
                    if (headers == null) {
                        return null;
                    }
                    Map<String, String> map = new LinkedHashMap<>();
                    headers.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
                    return map;
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }, blockingExecutor);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private static Throwable unwrapCompletion(Throwable ex) {
        return (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
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
                    log.warn().attr("uri", target.getUri())
                            .exceptionMessage(throwable)
                            .log("Failed to perform http put request");
                    future.completeExceptionally(getApiException(throwable.getCause()));
                }

            });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    public <T, R> void asyncPostRequestWithResponse(final WebTarget target, Entity<T> entity,
                                                                    InvocationCallback<R> callback) {
        try {
            request(target).async().post(entity, callback);
        } catch (PulsarAdminException cae) {
            callback.failed(cae);
        }
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
                    log.warn().attr("uri", target.getUri())
                            .exceptionMessage(throwable)
                            .log("Failed to perform http post request");
                    future.completeExceptionally(getApiException(throwable.getCause()));
                }

            });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    public <T> void asyncGetRequest(final WebTarget target, InvocationCallback<T> callback) {
        try {
            request(target).async().get(callback);
        } catch (PulsarAdminException cae) {
            callback.failed(cae);
        }
    }

    public <T> CompletableFuture<T> asyncGetRequest(final WebTarget target, FutureCallback<T> callback) {
        asyncGetRequest(target, (InvocationCallback<T>) callback);
        return callback.future();
    }

    protected <T> CompletableFuture<T> asyncGetRequest(final WebTarget target, Class<? extends T> type) {
        return asyncGetRequest(target, response -> response.readEntity(type));
    }

    protected <T> CompletableFuture<T> asyncGetRequest(final WebTarget target, GenericType<T> type) {
        return asyncGetRequest(target, response -> response.readEntity(type));
    }

    private <T> CompletableFuture<T> asyncGetRequest(final WebTarget target, Function<Response, T> readResponse) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        asyncGetRequest(target,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        int status = response.getStatus();
                        // Accept both 200 OK and 204 No Content as success
                        if (status != Response.Status.OK.getStatusCode()
                                && status != Response.Status.NO_CONTENT.getStatusCode()) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            try {
                                // Handle 204 No Content - no response body to read
                                if (status == Response.Status.NO_CONTENT.getStatusCode()) {
                                    future.complete(null);
                                } else {
                                    future.complete(readResponse.apply(response));
                                }
                            } catch (Exception e) {
                                future.completeExceptionally(getApiException(e));
                            }
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
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
                    log.warn().attr("uri", target.getUri())
                            .exceptionMessage(throwable)
                            .log("Failed to perform http delete request");
                    future.completeExceptionally(getApiException(throwable.getCause()));
                }
            });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    public <T> void asyncDeleteRequest(final WebTarget target, InvocationCallback<T> callback) {
        try {
            request(target).async().delete(callback);
        } catch (PulsarAdminException cae) {
            callback.failed(cae);
        }
    }

    public static PulsarAdminException getApiException(Throwable e) {
        if (e instanceof PulsarAdminException) {
            return (PulsarAdminException) e;
        } else if (e instanceof ServiceUnavailableException) {
            if (e.getCause() instanceof java.net.ConnectException) {
                return new ConnectException(e.getCause());
            } else {
                ServerErrorException see = (ServerErrorException) e;
                int statusCode = see.getResponse().getStatus();
                String httpError = getReasonFromServer(see);
                return new PulsarAdminException(e, httpError, statusCode);
            }
        } else if (e instanceof WebApplicationException) {
            // Handle 5xx exceptions
            if (e instanceof ServerErrorException) {
                ServerErrorException see = (ServerErrorException) e;
                int statusCode = see.getResponse().getStatus();
                String httpError = getReasonFromServer(see);
                return new ServerSideErrorException(see, httpError, httpError, statusCode);
            } else if (e instanceof ClientErrorException) {
                // Handle 4xx exceptions
                ClientErrorException cee = (ClientErrorException) e;
                int statusCode = cee.getResponse().getStatus();
                String httpError = getReasonFromServer(cee);
                switch (statusCode) {
                    case 401:
                    case 403:
                        return new NotAuthorizedException(cee, httpError, statusCode);
                    case 404:
                        return new NotFoundException(cee, httpError, statusCode);
                    case 405:
                        return new NotAllowedException(cee, httpError, statusCode);
                    case 409:
                        return new ConflictException(cee, httpError, statusCode);
                    case 412:
                        return new PreconditionFailedException(cee, httpError, statusCode);
                    default:
                        return new PulsarAdminException(httpError, cee, httpError, statusCode);
                }
            } else {
                WebApplicationException wae = (WebApplicationException) e;
                int statusCode = wae.getResponse().getStatus();
                String httpError = getReasonFromServer(wae);
                return new PulsarAdminException(httpError, wae, httpError, statusCode);
            }
        } else {
            return new PulsarAdminException(e);
        }
    }

    public PulsarAdminException getApiException(Response response) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
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

    public static String getReasonFromServer(WebApplicationException e) {
        try {
            return e.getResponse().readEntity(ErrorData.class).reason.toString();
        } catch (Exception ex) {
            try {
                return ObjectMapperFactory.getMapper().reader().readValue(
                        e.getResponse().getEntity().toString(), ErrorData.class).reason;
            } catch (Exception ex1) {
                try {
                    return ObjectMapperFactory.getMapper().reader()
                            .readValue(e.getMessage(), ErrorData.class).reason;
                } catch (Exception ex2) {
                    // could not parse output to ErrorData class
                    return e.getMessage();
                }
            }
        }
    }

    protected <T> T sync(Supplier<CompletableFuture<T>> executor) throws PulsarAdminException {
        try {
            return executor.get().get(this.requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
           Thread.currentThread().interrupt();
          throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
          throw new PulsarAdminException.TimeoutException(e);
        } catch (ExecutionException e) {
            // we want to have a stacktrace that points to this point, in order to return a meaningful
            // stacktrace to the user, otherwise we will have a stacktrace
            // related to another thread, because all Admin API calls are async
            throw PulsarAdminException.wrap(getApiException(e.getCause()));
        } catch (Exception e) {
            throw PulsarAdminException.wrap(getApiException(e));
        }
    }

    /**
     * InvocationCallback that creates a CompletableFuture and completes it based on the response.
     * Must be subclassed to provide runtime type information to the ReST client library.
     * @param <T> type to which the response body is parsed in case of success
     */
    abstract static class FutureCallback<T> implements InvocationCallback<T> {
        private final CompletableFuture<T> future = new CompletableFuture<>();

        @Override
        public void completed(T value) {
            future.complete(value);
        }

        @Override
        public void failed(Throwable throwable) {
            future.completeExceptionally(getApiException(throwable.getCause()));
        }

        public CompletableFuture<T> future() {
            return future;
        }

    }
}
