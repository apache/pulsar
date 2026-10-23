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
package org.apache.pulsar.client.impl.auth;

import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.sasl.SaslConstants.AUTH_METHOD_NAME;
import static org.apache.pulsar.common.sasl.SaslConstants.JAAS_CLIENT_SECTION_NAME;
import static org.apache.pulsar.common.sasl.SaslConstants.JAAS_DEFAULT_CLIENT_SECTION_NAME;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN_EXPIRED;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_BROKER_PROTOCOL;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_HEADER_STATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_HEADER_TYPE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_SERVER_TYPE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_CLIENT_INIT;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_COMPLETE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_NEGOTIATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER_CHECK_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_TYPE_VALUE;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Invocation.Builder;
import jakarta.ws.rs.client.InvocationCallback;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.security.auth.login.LoginException;
import lombok.CustomLog;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServicesAware;
import org.apache.pulsar.client.api.v5.internal.V5AuthenticationProvider;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.client.impl.auth.PulsarSaslClient.ClientCallbackHandler;
import org.apache.pulsar.client.impl.auth.v5.AsyncHttpAuthenticationProvider;
import org.apache.pulsar.client.impl.auth.v5.HttpAuthenticationDriver;
import org.apache.pulsar.client.impl.auth.v5.HttpChallengeTransport;
import org.apache.pulsar.client.impl.auth.v5.SaslAuthenticationV5;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.JAASCredentialsContainer;

/**
 * Authentication provider for SASL based authentication.
 *
 * SASL need config files through JVM parameter:
 *   a jaas.conf, which is set by `-Djava.security.auth.login.config=/dir/jaas.conf`
 *   for Kerberos a krb5.conf, which is set by `-Djava.security.krb5.conf=/dir/krb5.conf`
 */
@CustomLog
public class AuthenticationSasl
        implements Authentication, EncodedAuthenticationParameterSupport, V5AuthenticationProvider,
        AsyncHttpAuthenticationProvider, ClientAuthenticationServicesAware {
    private static final long serialVersionUID = 1L;
    // this is a static object that shares amongst client.
    private static JAASCredentialsContainer jaasCredentialsContainer;
    private static volatile boolean initializedJAAS = false;

    private Map<String, String> configuration;
    private String loginContextName;
    private String serverType = null;
    // PIP-478: the client's framework services, late-bound before start(); null until then.
    private transient volatile ClientAuthenticationServices authServices;
    // PIP-478: the framework HTTP auth driver for the SASL-over-HTTP flow, created lazily on the
    // first HTTP request after start(). Its default transport is this plugin's own JAX-RS client (the
    // faithful admin-path transport; the lookup path supplies its shared AsyncHttpClient instead).
    //
    // The driver and the services it was built with are ONE immutable value behind ONE volatile, not two
    // fields. Two independently-read volatiles admit a plain temporal interleaving — no reordering needed,
    // since volatile accesses are totally ordered: a reader can read the old driver, a rebuilding thread can
    // then publish both new fields, and the reader's subsequent services comparison passes against the new
    // value while it returns the driver built with the old one. That is precisely the mispairing this cache
    // exists to prevent, and both HTTP call sites hit this method per request, so concurrent readers are the
    // normal case for a shared client+admin. Pairing them makes the stale combination unrepresentable.
    private transient volatile HttpDriverBinding httpDriverBinding;

    /**
     * A driver together with the services it was built with — read and published as one value.
     *
     * @param driver   the HTTP authentication driver
     * @param services the services the driver was built with; may be {@code null} before any bind
     */
    private record HttpDriverBinding(HttpAuthenticationDriver driver, ClientAuthenticationServices services) {
    }

    // The v5 body the HTTP driver wraps, kept across driver rebuilds so its cached role token survives a
    // services rebind. Guarded by the same monitor as httpDriverBinding.
    private transient SaslAuthenticationV5 httpAuthenticationBody;

    public AuthenticationSasl() {
    }

    @Override
    public String getAuthMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public AuthenticationDataProvider getAuthData(String serverHostname) throws PulsarClientException {
        // reuse this to return a DataProvider which contains a SASL client
        try {
            PulsarSaslClient saslClient = new PulsarSaslClient(serverHostname, serverType,
                    jaasCredentialsContainer.getSubject());
            return new SaslAuthenticationDataProvider(saslClient);
        } catch (Throwable t) {
            log.error().exception(t).log("Failed create sasl client");
            throw new PulsarClientException(t);
        }
    }

    @Override
    public void bindClientAuthenticationServices(ClientAuthenticationServices services) {
        this.authServices = services;
    }

    @Override
    public org.apache.pulsar.client.api.v5.auth.Authentication v5Authentication() {
        // PIP-478: the client drives this v5-native body over the binary transport. One body serves the
        // client; the per-broker PulsarSaslClient that carries a multi-round handshake lives in the
        // exchange's call-context state slot, not here. The SASL-over-HTTP loop stays on this shim, driven
        // through httpAuthenticationDriver() below.
        return new SaslAuthenticationV5(new ShimSaslProviderFactory(this));
    }

    @Override
    public Optional<HttpAuthenticationDriver> httpAuthenticationDriver() {
        // PIP-478: expose the v5-native SASL-over-HTTP body to the framework HTTP auth driver so the
        // HTTP callers route the 401->resubmit->200 exchange through the shared state machine instead of the
        // deprecated authenticationStage(...) hook (which stays for third-party v4 plugins). The default
        // transport is this plugin's own JAX-RS client — exactly what authenticationStage(...) uses today.
        // The driver captures the services it is built with, so caching it across a rebind would freeze
        // whichever binding happened to come first. One plugin instance is routinely shared between a
        // PulsarClient and a PulsarAdmin, and both bind: caching the first would hand one of them the
        // other's services for the rest of its life. Rebuild the driver when the binding changes.
        //
        // The BODY is deliberately not rebuilt with it. It holds the validated role token across requests
        // (SaslAuthenticationV5.cachedRoleToken), which exists precisely so a request replays that token
        // instead of restarting a full Kerberos negotiation. Minting a fresh body on every rebind would
        // throw it away and force a renegotiation — and this method's own scenario, one plugin shared
        // between a client and an admin, is exactly when rebinds happen.
        ClientAuthenticationServices services = this.authServices;
        // One read, so the driver and the services it was compared against cannot come from different
        // publications. See the field comment.
        HttpDriverBinding binding = httpDriverBinding;
        if (binding != null && binding.services() == services) {
            return Optional.of(binding.driver());
        }
        synchronized (this) {
            binding = httpDriverBinding;
            if (binding == null || binding.services() != services) {
                if (httpAuthenticationBody == null) {
                    httpAuthenticationBody = new SaslAuthenticationV5(new ShimSaslProviderFactory(this));
                }
                binding = new HttpDriverBinding(new HttpAuthenticationDriver(
                        httpAuthenticationBody, services, new JaxRsChallengeTransport()), services);
                httpDriverBinding = binding;
            }
            return Optional.of(binding.driver());
        }
    }

    /** Copy a response's first-value headers into the v5 header carrier. */
    private static HttpAuthHeaders toHeaders(Response response) {
        Map<String, String> headers = new LinkedHashMap<>();
        response.getStringHeaders().forEach((name, values) -> {
            if (values != null && !values.isEmpty() && values.get(0) != null) {
                headers.put(name, values.get(0));
            }
        });
        return HttpAuthHeaders.of(headers);
    }

    /**
     * Complete {@code future} from {@code response} and always close the response.
     *
     * <p>{@code InvocationCallback<Response>} hands the caller an unclosed response, and reading only its
     * headers neither consumes the entity nor releases the connection — so completing without closing leaks
     * one pooled connection per authentication round. The driver runs at least one round on every admin
     * request (round 0 replays the cached role token), so the leak is per request, not per client. The late
     * case — the future already timed out or was cancelled — must close too, which is the only case the
     * original code handled.
     *
     * <p>Package-private (VisibleForTesting) so the close contract can be asserted on both branches.
     *
     * @param future   the future to complete
     * @param response the JAX-RS response, always closed before returning
     */
    static void completeAndClose(CompletableFuture<HttpChallengeTransport.Result> future, Response response) {
        try {
            future.complete(new HttpChallengeTransport.Result(response.getStatus(), toHeaders(response)));
        } finally {
            response.close();
        }
    }

    /**
     * A {@link HttpChallengeTransport} backed by this plugin's own JAX-RS {@link Client} (created in
     * {@link #start()}) — the faithful transport for the admin-path SASL warmup, matching what
     * {@code authenticationStage(...)} does today. Each round is a bodiless {@code GET} to the original URI.
     * The JAX-RS client itself is created with default, unbounded timeouts, so the per-request {@code timeout}
     * is enforced here by bounding the returned future ({@link CompletableFuture#orTimeout}) AND cancelling the
     * underlying JAX-RS {@link Future} on timeout/failure — the request is bounded, so a peer that accepts the
     * connection but never responds cannot leak an in-flight request (fd/socket exhaustion) across retries. A
     * response that arrives after the future already timed out is closed rather than leaked.
     */
    private final class JaxRsChallengeTransport implements HttpChallengeTransport {
        @Override
        public CompletableFuture<Result> get(URI uri, HttpAuthHeaders requestHeaders, Duration timeout) {
            CompletableFuture<Result> future = new CompletableFuture<>();
            try {
                Client c = client;
                if (c == null) {
                    throw new IllegalStateException("SASL authentication HTTP client is not started");
                }
                Builder builder = c.target(uri).request(MediaType.APPLICATION_JSON);
                requestHeaders.asMap().forEach(builder::header);
                Future<Response> responseFuture = builder.async().get(new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        completeAndClose(future, response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                });
                if (timeout != null && !timeout.isNegative() && !timeout.isZero()) {
                    future.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS);
                }
                // Cancel the unbounded JAX-RS request on timeout/failure so its socket is released, not leaked.
                // cancel(false), not cancel(true): the isDone() guard assumes Jersey marks its Future done
                // before invoking InvocationCallback.completed(...), and if it does not, an interrupting
                // cancel would fire against the worker thread that just delivered a successful response.
                // Not interrupting is immune to that ordering either way, and interruption buys nothing here
                // — releasing the request is what the cancel is for.
                future.whenComplete((result, throwable) -> {
                    if (!responseFuture.isDone()) {
                        responseFuture.cancel(false);
                    }
                });
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
            return future;
        }

    }

    private static final class ShimSaslProviderFactory implements SaslAuthenticationV5.SaslProviderFactory {
        private static final long serialVersionUID = 1L;
        private final AuthenticationSasl shim;

        ShimSaslProviderFactory(AuthenticationSasl shim) {
            this.shim = shim;
        }

        @Override
        public AuthenticationDataProvider create(String brokerHost) throws Exception {
            return shim.getAuthData(brokerHost);
        }
    }

    @Override
    public void configure(String encodedAuthParamString) {
        if (isBlank(encodedAuthParamString)) {
            log.info().attr("defaultSectionName", JAAS_DEFAULT_CLIENT_SECTION_NAME)
                    .log("authParams for SASL is empty, will use default JAAS client section name");
        }

        try {
            setAuthParams(AuthenticationUtil.configureFromJsonString(encodedAuthParamString));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse SASL authParams", e);
        }
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
        try {
            setAuthParams(authParams);
        }  catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse SASL authParams", e);
        }
    }

    // use passed in parameter to config ange get jaasCredentialsContainer.
    private void setAuthParams(Map<String, String> authParams) throws PulsarClientException {
        this.configuration = authParams;

        // read section from config files of kerberos
        this.loginContextName = authParams
            .getOrDefault(JAAS_CLIENT_SECTION_NAME, JAAS_DEFAULT_CLIENT_SECTION_NAME);
        this.serverType = authParams
            .getOrDefault(SASL_SERVER_TYPE, SASL_BROKER_PROTOCOL);

        // init the static jaasCredentialsContainer that shares amongst client.
        if (!initializedJAAS) {
            synchronized (this) {
                if (jaasCredentialsContainer == null) {
                    log.info().attr("loginContext", loginContextName).log("JAAS loginContext");
                    try {
                        jaasCredentialsContainer = new JAASCredentialsContainer(
                            loginContextName,
                            new ClientCallbackHandler(),
                            configuration);
                        initializedJAAS = true;
                    } catch (LoginException e) {
                        log.error().exception(e).log("JAAS login in client failed");
                        throw new PulsarClientException(e);
                    }
                }
            }
        }
    }

    @Override
    public void start() throws PulsarClientException {
        client = ClientBuilder.newClient();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
        // Drop the cached HTTP driver; its default transport captured the now-closed JAX-RS client. The body
        // goes with it, so the role token it cached does not outlive the plugin that authenticated for it.
        httpDriverBinding = null;
        synchronized (this) {
            httpAuthenticationBody = null;
        }
        if (jaasCredentialsContainer != null) {
            jaasCredentialsContainer.close();
            jaasCredentialsContainer = null;
            initializedJAAS = false;
        }
    }

    // PIP-478: written by start()/close() on the application thread and read from the HTTP challenge
    // driver's continuation threads (Jersey async callbacks), so both need a happens-before edge —
    // matching the volatile treatment already given to authServices / httpAuthenticationDriver.
    private volatile String saslRoleToken = null;
    private volatile Client client = null;

    // role token exists but expired return true
    private boolean isRoleTokenExpired(Map<String, String> responseHeaders) {
        if ((saslRoleToken != null)
            && (responseHeaders != null)
            // header type match
            && (responseHeaders.get(SASL_HEADER_TYPE) != null && responseHeaders.get(SASL_HEADER_TYPE)
                    .equalsIgnoreCase(SASL_TYPE_VALUE))
            // header state expired
            && (responseHeaders.get(SASL_HEADER_STATE) != null && responseHeaders.get(SASL_HEADER_STATE)
                    .equalsIgnoreCase(SASL_AUTH_ROLE_TOKEN_EXPIRED))) {
            return true;
        } else {
            return false;
        }
    }

    @SneakyThrows(Exception.class)
    private Builder newRequestBuilder(WebTarget target,
                                      AuthenticationDataProvider authData,
                                      Map<String, String> previousResHeaders) {
        Builder builder = target.request(MediaType.APPLICATION_JSON);
        Set<Entry<String, String>>  headers = newRequestHeader(
            target.getUri().toString(),
            authData,
            previousResHeaders);

        headers.forEach(entry -> {
            builder.header(entry.getKey(), entry.getValue());
        });
        return builder;
    }

    // set header according to previous response
    @Override
    public Set<Entry<String, String>> newRequestHeader(String hostName,
                                                       AuthenticationDataProvider authData,
                                                       Map<String, String> previousRespHeaders) throws Exception {

        Map<String, String> headers = new HashMap<>();

        if (authData.hasDataForHttp()) {
            authData.getHttpHeaders().forEach(header ->
                headers.put(header.getKey(), header.getValue())
            );
        }

        // role token expired in last check. remove role token, new sasl client, restart auth.
        if (isRoleTokenExpired(previousRespHeaders)) {
            previousRespHeaders = null;
            saslRoleToken = null;
            authData = getAuthData(hostName);
        }

        // role token is not expired and OK to use.
        // 1. first time request, send server to check if expired.
        // 2. server checked, and return SASL_STATE_COMPLETE, ask server to complete auth
        // 3. server checked, and not return SASL_STATE_COMPLETE
        if (saslRoleToken != null) {
            headers.put(SASL_AUTH_ROLE_TOKEN, saslRoleToken);
            if (previousRespHeaders == null) {
                // first time auth, ask server to check the role token expired or not.
                log.debug("request builder add token: Check token");
                headers.put(SASL_HEADER_STATE, SASL_STATE_SERVER_CHECK_TOKEN);
            } else if (previousRespHeaders.get(SASL_HEADER_STATE).equalsIgnoreCase(SASL_STATE_COMPLETE)) {
                headers.put(SASL_HEADER_STATE, SASL_STATE_COMPLETE);
                log.debug("request builder add token. role verified by server");
            } else {
                log.debug().attr("state", previousRespHeaders.get(SASL_HEADER_STATE))
                        .log("request builder add token. NOT complete");
                headers.put(SASL_HEADER_STATE, SASL_STATE_NEGOTIATE);
            }
            return headers.entrySet();
        }

        // role token is null, need do auth.
        if (previousRespHeaders == null) {
            log.debug("Init authn in client side");
            // first time init
            headers.put(SASL_HEADER_STATE, SASL_STATE_CLIENT_INIT);
            AuthData initData = authData.authenticate(AuthData.INIT_AUTH_DATA);
            headers.put(SASL_AUTH_TOKEN,
                Base64.getEncoder().encodeToString(initData.getBytes()));
        } else {
            AuthData brokerData = AuthData.of(
                Base64.getDecoder().decode(
                    previousRespHeaders.get(SASL_AUTH_TOKEN)));
            AuthData clientData = authData.authenticate(brokerData);

            headers.put(SASL_STATE_SERVER, previousRespHeaders.get(SASL_STATE_SERVER));
            headers.put(SASL_HEADER_TYPE, SASL_TYPE_VALUE);
            headers.put(SASL_HEADER_STATE, SASL_STATE_NEGOTIATE);
            headers.put(SASL_AUTH_TOKEN,
                Base64.getEncoder().encodeToString(clientData.getBytes()));
        }

        return headers.entrySet();
    }

    private Map<String, String> getHeaders(Response response) {
        Map<String, String> headers = new HashMap<>();
        String saslHeader = response.getHeaderString(SASL_HEADER_TYPE);
        String headerState = response.getHeaderString(SASL_HEADER_STATE);
        String authToken = response.getHeaderString(SASL_AUTH_TOKEN);
        String serverStateId = response.getHeaderString(SASL_STATE_SERVER);

        if (saslRoleToken != null) {
            headers.put(SASL_AUTH_ROLE_TOKEN, saslRoleToken);
        }

        headers.put(SASL_HEADER_TYPE, saslHeader);
        headers.put(SASL_HEADER_STATE, headerState);
        headers.put(SASL_AUTH_TOKEN, authToken);
        headers.put(SASL_STATE_SERVER, serverStateId);
        return headers;
    }

    @Override
    public void authenticationStage(String requestUrl,
                                    AuthenticationDataProvider authData,
                                    Map<String, String> previousResHeaders,
                                    CompletableFuture<Map<String, String>> authFuture) {
        // a new request for sasl auth
        Builder builder = newRequestBuilder(client.target(requestUrl), authData, previousResHeaders);
        builder.async().get(new InvocationCallback<Response>() {
            @Override
            public void completed(Response response) {
                if (response.getStatus() == HTTP_UNAUTHORIZED) {
                    // sasl auth on going
                    authenticationStage(requestUrl, authData, getHeaders(response), authFuture);
                    return;
                }

                if (response.getStatus() != HttpURLConnection.HTTP_OK) {
                    log.warn().attr("status", response.getStatusInfo())
                            .log("HTTP get request failed");
                    authFuture.completeExceptionally(new PulsarClientException("Sasl Auth request failed: "
                            + response.getStatus()));
                    return;
                } else {
                    if (response.getHeaderString(SASL_AUTH_ROLE_TOKEN) != null) {
                        saslRoleToken = response.getHeaderString(SASL_AUTH_ROLE_TOKEN);
                    }

                    log.debug().attr("saslRoleToken", saslRoleToken)
                            .log("Complete auth with saslRoleToken");
                    authFuture.complete(getHeaders(response));
                    return;
                }
            }

            @Override
            public void failed(Throwable throwable) {
                log.warn().exception(throwable).log("Failed to perform http request");
                authFuture.completeExceptionally(new PulsarClientException(throwable));
                return;
            }
        });
    }
}
