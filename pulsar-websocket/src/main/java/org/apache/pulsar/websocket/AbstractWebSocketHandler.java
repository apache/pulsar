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
package org.apache.pulsar.websocket;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.client.api.PulsarClientException.AuthenticationException;
import org.apache.pulsar.client.api.PulsarClientException.AuthorizationException;
import org.apache.pulsar.client.api.PulsarClientException.ConsumerBusyException;
import org.apache.pulsar.client.api.PulsarClientException.IncompatibleSchemaException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBlockedQuotaExceededError;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBlockedQuotaExceededException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerFencedException;
import org.apache.pulsar.client.api.PulsarClientException.TimeoutException;
import org.apache.pulsar.client.api.PulsarClientException.TooManyRequestsException;
import org.apache.pulsar.client.api.PulsarClientException.TopicDoesNotExistException;
import org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.AuthChallenge;
import org.apache.pulsar.websocket.data.AuthResponse;
import org.apache.pulsar.websocket.data.Challenge;
import org.apache.pulsar.websocket.data.CommandAuthChallenge;
import org.apache.pulsar.websocket.data.CommandAuthResponse;
import org.apache.pulsar.websocket.data.CommandError;
import org.apache.pulsar.websocket.data.ConsumerCommand;
import org.apache.pulsar.websocket.data.ServerError;
import org.apache.pulsar.websocket.data.protocol.PulsarWebsocketDecoder;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractWebSocketHandler extends PulsarWebsocketDecoder implements Closeable {

    protected final WebSocketService service;
    protected final HttpServletRequest request;

    protected TopicName topic;
    protected final Map<String, String> queryParams;
    protected final ObjectReader consumerCommandReader =
            ObjectMapperFactory.getMapper().reader().forType(ConsumerCommand.class);
    private final String anonymousUserRole;

    private AuthenticationState authState;
    private AuthenticationDataSource authData;
    private String authRole = null;
    private String authMethod = "none";
    private boolean pendingAuthChallengeResponse = false;
    private int clientProtocolVersion = 0;

    private ScheduledFuture<?> pingFuture;
    private ScheduledFuture<?> authRefreshFuture;

    public AbstractWebSocketHandler(WebSocketService service,
                                    HttpServletRequest request,
                                    ServletUpgradeResponse response) {
        this.service = service;
        this.request = new WebSocketHttpServletRequestWrapper(request);
        this.anonymousUserRole = service.getConfig().getAnonymousUserRole();

        this.queryParams = new TreeMap<>();
        request.getParameterMap().forEach((key, values) -> {
            queryParams.put(key, values[0]);
        });
        extractTopicName(request);
    }

    private String getAuthMethodName(HttpServletRequest request) {
        return request.getHeader(AuthenticationFilter.PULSAR_AUTH_METHOD_NAME);
    }

    private boolean checkAuthentication(ServletUpgradeResponse response) {
        if (!service.isAuthenticationEnabled()) {
            return true;
        }
        try {
            String authMethodNameHeader = getAuthMethodName(request);

            if (authMethodNameHeader != null) {
                AuthenticationProvider providerToUse = service.getAuthenticationService()
                        .getAuthenticationProvider(authMethodNameHeader);
                try {
                    AuthenticationState authenticationState = providerToUse.newHttpAuthState(request);
                    authData = authenticationState.getAuthDataSource();
                    authRole = providerToUse.authenticateAsync(authData).get();
                    authState = authenticationState;
                    authMethod = authMethodNameHeader;
                    return true;
                } catch (javax.naming.AuthenticationException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Authentication failed for provider " + authMethodNameHeader + " : "
                                + e.getMessage(), e);
                    }
                    throw e;
                } catch (ExecutionException | InterruptedException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Authentication failed for provider " + authMethodNameHeader + " : "
                                + e.getMessage(), e);
                    }
                    throw new RuntimeException(e);
                }
            } else {
                Set<String> authMethodNames = service.getAuthenticationService().getAuthMethodNames();
                for (String authMethodName : authMethodNames) {
                    try {
                        AuthenticationProvider provider = service.getAuthenticationService()
                                .getAuthenticationProvider(authMethodName);
                        AuthenticationState authenticationState = provider.newHttpAuthState(request);
                        String authenticationRole = provider
                                .authenticateAsync(authenticationState.getAuthDataSource())
                                .get();

                        authState = authenticationState;
                        authRole = authenticationRole;
                        authMethod = authMethodName;
                        return true;
                    } catch (ExecutionException | InterruptedException | javax.naming.AuthenticationException e) {
                        if (log.isDebugEnabled()) {
                            log.debug("Authentication failed for provider " + authMethodName + ": "
                                    + e.getMessage(), e);
                        }
                        // Ignore the exception because we don't know which authentication method is
                        // expected here.
                    }
                }

                if (StringUtils.isNotBlank(anonymousUserRole)) {
                    authRole = anonymousUserRole;
                    if (log.isDebugEnabled()) {
                        log.debug("Anonymous authentication succeded");
                    }
                    return true;
                }
            }
            log.info("[{}:{}] Authenticated WebSocket client {} on topic {}", request.getRemoteAddr(),
                    request.getRemotePort(), authRole, topic);
        } catch (javax.naming.AuthenticationException e) {
            log.warn("[{}:{}] Failed to authenticated WebSocket client {} on topic {}: {}", request.getRemoteAddr(),
                    request.getRemotePort(), authRole, topic, e.getMessage());
        }
        try {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Failed to authenticate");
        } catch (IOException e1) {
            log.warn("[{}:{}] Failed to send error: {}", request.getRemoteAddr(), request.getRemotePort(),
                    e1.getMessage(), e1);
        }
        return false;
    }

    private boolean checkAuthorization(ServletUpgradeResponse response) {
        if (!service.isAuthorizationEnabled()) {
            return true;
        }

        AuthenticationDataSource authenticationData;
        if (authState != null) {
            authenticationData = authState.getAuthDataSource();
        } else {
            authenticationData = new AuthenticationDataHttps(request);
        }
        try {
            if (isAuthorized(authRole, authenticationData)) {
                return true;
            }
        } catch (Exception e) {
            log.warn("[{}:{}] Got an exception when authorizing WebSocket client {} on topic {} on: {}",
                    request.getRemoteAddr(), request.getRemotePort(), authRole, topic, e.getMessage());
            try {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Server error");
            } catch (IOException e1) {
                log.warn("[{}:{}] Failed to send error: {}", request.getRemoteAddr(), request.getRemotePort(),
                        e1.getMessage(), e1);
            }
            return false;
        }

        try {
            log.warn("[{}:{}] WebSocket Client [{}] is not authorized on topic {}", request.getRemoteAddr(),
                    request.getRemotePort(), authRole, topic);
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "Not authorized");
        } catch (IOException e1) {
            log.warn("[{}:{}] Failed to send error: {}", request.getRemoteAddr(), request.getRemotePort(),
                    e1.getMessage(), e1);
        }
        return false;
    }

    protected boolean checkAuth(ServletUpgradeResponse response) {
        return checkAuthentication(response) && checkAuthorization(response);
    }

    protected static int getErrorCode(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return HttpServletResponse.SC_BAD_REQUEST;
        } else if (e instanceof AuthenticationException) {
            return HttpServletResponse.SC_UNAUTHORIZED;
        } else if (e instanceof AuthorizationException) {
            return HttpServletResponse.SC_FORBIDDEN;
        } else if (e instanceof NotFoundException || e instanceof TopicDoesNotExistException) {
            return HttpServletResponse.SC_NOT_FOUND;
        } else if (e instanceof ProducerBusyException || e instanceof ConsumerBusyException
                || e instanceof ProducerFencedException || e instanceof IncompatibleSchemaException) {
            return HttpServletResponse.SC_CONFLICT;
        } else if (e instanceof TooManyRequestsException) {
            return 429; // Too Many Requests
        } else if (e instanceof ProducerBlockedQuotaExceededError || e instanceof ProducerBlockedQuotaExceededException
                || e instanceof TopicTerminatedException) {
            return HttpServletResponse.SC_SERVICE_UNAVAILABLE;
        } else if (e instanceof TimeoutException) {
            return HttpServletResponse.SC_GATEWAY_TIMEOUT;
        } else {
            return HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        }
    }

    protected static String getErrorMessage(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return "Invalid query params: " + e.getMessage();
        } else {
            return "Failed to create producer/consumer: " + e.getMessage();
        }
    }

    private void closePingFuture() {
        if (pingFuture != null && !pingFuture.isDone()) {
            pingFuture.cancel(true);
        }
    }

    private void closeAuthRefreshFuture() {
        if (authRefreshFuture != null && !authRefreshFuture.isDone()) {
            authRefreshFuture.cancel(true);
        }
    }

    public static CommandAuthChallenge newAuthChallenge(String authMethodName, AuthData brokerData,
            int protocolVersion) {

        CommandAuthChallenge commandAuthChallenge = new CommandAuthChallenge();
        AuthChallenge authChallenge = new AuthChallenge();
        Challenge challenge = new Challenge(authMethodName, authMethodName);

        authChallenge.setProtocolVersion(protocolVersion);
        commandAuthChallenge.setAuthChallenge(authChallenge);

        authChallenge.setChallenge(challenge);

        return commandAuthChallenge;
    }

    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);
        int webSocketPingDurationSeconds = service.getConfig().getWebSocketPingDurationSeconds();
        if (webSocketPingDurationSeconds > 0) {
            pingFuture = service.getExecutor().scheduleAtFixedRate(() -> {
                try {
                    session.getRemote().sendPing(ByteBuffer.wrap("PING".getBytes(StandardCharsets.UTF_8)));
                } catch (IOException e) {
                    log.warn("[{}] WebSocket send ping", getSession().getRemoteAddress(), e);
                }
            }, webSocketPingDurationSeconds, webSocketPingDurationSeconds, TimeUnit.SECONDS);
        }
        int authenticationRefreshCheckSeconds = service.getConfig().getAuthenticationRefreshCheckSeconds();
        if (authenticationRefreshCheckSeconds > 0) {
            authRefreshFuture = service.getExecutor().scheduleAtFixedRate(() -> {
                try {
                    if (!authState.isExpired()) {
                        // Credentials are still valid. Nothing to do at this point
                        return;
                    }

                    if (pendingAuthChallengeResponse) {
                        log.warn("[{}] Closing connection after timeout on refreshing auth credentials",
                                session.getRemoteAddress());
                        session.close();
                        return;
                    }

                    log.info("[{}] Refreshing authentication credentials for authRole {}",
                            getSession().getRemoteAddress(), this.authRole);
                    AuthData brokerData = authState.refreshAuthentication();
                    CommandAuthChallenge commandAuthChallenge = newAuthChallenge(authMethod, brokerData,
                            clientProtocolVersion);
                    String commandAuthChallengeString = objectWriter().writeValueAsString(commandAuthChallenge);

                    session.getRemote().sendString(commandAuthChallengeString);

                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Sent auth challenge to client to refresh credentials with method: {}.",
                                getSession().getRemoteAddress(), authMethod);
                    }

                    pendingAuthChallengeResponse = true;
                } catch (javax.naming.AuthenticationException e) {
                    log.warn("[{}] Failed to refresh authentication: {}", getSession().getRemoteAddress(), e);
                    session.close();
                } catch (Exception e) {
                    log.warn("[{}] Websocket Auth refresh general Exception", getSession().getRemoteAddress(), e);

                }
            }, authenticationRefreshCheckSeconds, authenticationRefreshCheckSeconds, TimeUnit.SECONDS);
        }
        log.info("[{}] New WebSocket session on topic {}", session.getRemoteAddress(), topic);
    }

    @Override
    protected void handleAuthResponse(CommandAuthResponse commandAuthResponse) {
        AuthResponse authResponse = commandAuthResponse.getAuthResponse();
        pendingAuthChallengeResponse = false;

        if (log.isDebugEnabled()) {
            log.debug("Received AuthResponse from {}, auth method: {}",
                    getRemote().getInetSocketAddress(), authResponse.getResponse().getAuthMethodName());
        }

        try {
            AuthData clientData = AuthData
                    .of(authResponse.getResponse().getAuthData().getBytes(StandardCharsets.UTF_8));
            doAuthentication(clientData, false, authResponse.getProtocolVersion(),
                    authResponse.hasClientVersion() ? authResponse.getClientVersion() : EMPTY);
        } catch (Exception e) {
            log.warn("[{}] Websocket handleAuthResponse general Exception", getSession().getRemoteAddress(), e);
            authenticationFailed(e);
        }
    }

    // According to auth result, send Connected, AuthChallenge, or Error command.
    private void doAuthentication(AuthData clientData,
            boolean useOriginalAuthState,
            int clientProtocolVersion,
            final String clientVersion) {

        if (log.isDebugEnabled()) {
            log.debug("Authenticate using original auth state : {}, role = {}", useOriginalAuthState, authRole);
        }

        authState
                .authenticateAsync(clientData)
                .whenCompleteAsync((authChallenge, throwable) -> {
                    if (throwable != null) {
                        authenticationFailed(throwable);
                    }
                }, service.getExecutor());
    }

    // Handle authentication and authentication refresh failures. Must be called
    // from event loop.
    private void authenticationFailed(Throwable t) {
        try {
            CommandError commandError = new CommandError(-1, ServerError.AuthenticationError, "Failed to authenticate");
            String commandErrorString = objectWriter().writeValueAsString(commandError);
            getSession().getRemote().sendString(commandErrorString);
        } catch (JsonProcessingException e) {
            log.error("[{}] Error in sending authentication failure message: {}",
                    getRemote().getInetSocketAddress(), e);
        } catch (IOException e) {
            log.error("[{}] Error in sending authentication failure message: {}",
                    getRemote().getInetSocketAddress(), e);
        }
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        super.onWebSocketError(cause);
        log.info("[{}] WebSocket error on topic {} : {}", getSession().getRemoteAddress(), topic, cause.getMessage());
        try {
            closePingFuture();
            closeAuthRefreshFuture();
            close();
        } catch (IOException e) {
            log.error("Failed in closing WebSocket session for topic {} with error: {}", topic, e.getMessage());
        }
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        log.info("[{}] Closed WebSocket session on topic {}. status: {} - reason: {}", getSession().getRemoteAddress(),
                topic, statusCode, reason);
        try {
            closePingFuture();
            closeAuthRefreshFuture();
            close();
        } catch (IOException e) {
            log.warn("[{}] Failed to close handler for topic {}. ", getSession().getRemoteAddress(), topic, e);
        }
    }

    public void close(WebSocketError error) {
        log.warn("[{}] Closing WebSocket session for topic {} - code: [{}], reason: [{}]",
                getSession().getRemoteAddress(), topic, error.getCode(), error.getDescription());
        getSession().close(error.getCode(), error.getDescription());
    }

    public void close(WebSocketError error, String message) {
        log.warn("[{}] Closing WebSocket session for topic {} - code: [{}], reason: [{}]",
                getSession().getRemoteAddress(), topic, error.getCode(), error.getDescription() + ": " + message);
        getSession().close(error.getCode(), error.getDescription() + ": " + message);
    }

    protected String checkAuthentication() {
        return null;
    }

    protected void extractTopicName(HttpServletRequest request) {
        String uri = request.getRequestURI();
        List<String> parts = Splitter.on("/").splitToList(uri);

        // V1 Format must be like :
        // /ws/producer/persistent/my-property/my-cluster/my-ns/my-topic
        // or
        // /ws/consumer/persistent/my-property/my-cluster/my-ns/my-topic/my-subscription
        // or
        // /ws/reader/persistent/my-property/my-cluster/my-ns/my-topic

        // V2 Format must be like :
        // /ws/v2/producer/persistent/my-property/my-ns/my-topic
        // or
        // /ws/v2/consumer/persistent/my-property/my-ns/my-topic/my-subscription
        // or
        // /ws/v2/reader/persistent/my-property/my-ns/my-topic

        checkArgument(parts.size() >= 8, "Invalid topic name format");
        checkArgument(parts.get(1).equals("ws"));

        final boolean isV2Format = parts.get(2).equals("v2");
        final int domainIndex = isV2Format ? 4 : 3;
        checkArgument(parts.get(domainIndex).equals("persistent")
                || parts.get(domainIndex).equals("non-persistent"));

        final String domain = parts.get(domainIndex);
        final NamespaceName namespace = isV2Format ? NamespaceName.get(parts.get(5), parts.get(6)) :
                NamespaceName.get(parts.get(4), parts.get(5), parts.get(6));

        // The topic name which contains slashes is also split, so it needs to be jointed
        int startPosition = 7;
        boolean isConsumer = "consumer".equals(parts.get(2)) || "consumer".equals(parts.get(3));
        int endPosition = isConsumer ? parts.size() - 1 : parts.size();
        StringBuilder topicName = new StringBuilder(parts.get(startPosition));
        while (++startPosition < endPosition) {
            if (StringUtils.isEmpty(parts.get(startPosition))) {
               continue;
            }
            topicName.append("/").append(parts.get(startPosition));
        }
        final String name = Codec.decode(topicName.toString());

        topic = TopicName.get(domain, namespace, name);
    }

    @VisibleForTesting
    public ScheduledFuture<?> getPingFuture() {
        return pingFuture;
    }

    @VisibleForTesting
    public ScheduledFuture<?> getAuthRefreshFuture() {
        return authRefreshFuture;
    }

    protected abstract Boolean isAuthorized(String authRole,
                                            AuthenticationDataSource authenticationData) throws Exception;

    private static final Logger log = LoggerFactory.getLogger(AbstractWebSocketHandler.class);

    protected ObjectWriter objectWriter() {
        return ObjectMapperFactory.getMapper().writer();
    }
}
