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
package org.apache.pulsar.websocket;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.naming.AuthenticationException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.naming.DestinationName;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

public abstract class AbstractWebSocketHandler extends WebSocketAdapter implements Closeable {

    protected final WebSocketService service;
    protected final HttpServletRequest request;

    protected final String topic;
    protected final Map<String, String> queryParams;
    protected final boolean authResult;

    public AbstractWebSocketHandler(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
        this.service = service;
        this.request = request;
        this.topic = extractTopicName(request);

        this.queryParams = new TreeMap<>();
        request.getParameterMap().forEach((key, values) -> {
            queryParams.put(key, values[0]);
        });

        authResult = checkAuth(response);
    }

    private boolean checkAuth(ServletUpgradeResponse response) {
        String authRole = "<none>";
        AuthenticationDataSource authenticationData = new AuthenticationDataHttps(request);
        if (service.isAuthenticationEnabled()) {
            try {
                authRole = service.getAuthenticationService().authenticateHttpRequest(request);
                log.info("[{}:{}] Authenticated WebSocket client {} on topic {}", request.getRemoteAddr(),
                        request.getRemotePort(), authRole, topic);

            } catch (AuthenticationException e) {
                log.warn("[{}:{}] Failed to authenticated WebSocket client {} on topic {}: {}", request.getRemoteAddr(),
                        request.getRemotePort(), authRole, topic, e.getMessage());
                try {
                    response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Failed to authenticate");
                } catch (IOException e1) {
                    log.warn("[{}:{}] Failed to send error: {}", request.getRemoteAddr(), request.getRemotePort(),
                            e1.getMessage(), e1);
                }
                return false;
            }
        }

        if (service.isAuthorizationEnabled()) {
            try {
                if (!isAuthorized(authRole, authenticationData)) {
                    log.warn("[{}:{}] WebSocket Client [{}] is not authorized on topic {}", request.getRemoteAddr(),
                            request.getRemotePort(), authRole, topic);
                    response.sendError(HttpServletResponse.SC_FORBIDDEN, "Not authorized");
                    return false;
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
        }
        return true;
    }

    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);
        log.info("[{}] New WebSocket session on topic {}", session.getRemoteAddress(), topic);
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        super.onWebSocketError(cause);
        log.info("[{}] WebSocket error on topic {} : {}", getSession().getRemoteAddress(), topic, cause.getMessage());
        try {
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
            close();
        } catch (IOException e) {
            log.warn("[{}] Failed to close handler for topic {}. ", getSession().getRemoteAddress(), topic, e);
        }
    }

    public void close(WebSocketError error) {
        log.warn("[{}] Closing WebSocket session for topic {} - code: [{}], reason: [{}]",
                getSession().getRemoteAddress(), topic, error);
        getSession().close(error.getCode(), error.getDescription());
    }

    public void close(WebSocketError error, String message) {
        log.warn("[{}] Closing WebSocket session for topic {} - code: [{}], reason: [{}]",
                getSession().getRemoteAddress(), topic, error, message);
        getSession().close(error.getCode(), error.getDescription() + ": " + message);
    }

    protected String checkAuthentication() {
        return null;
    }

    private String extractTopicName(HttpServletRequest request) {
        String uri = request.getRequestURI();
        List<String> parts = Splitter.on("/").splitToList(uri);

        // Format must be like :
        // /ws/producer/persistent/my-property/my-cluster/my-ns/my-topic
        // or
        // /ws/consumer/persistent/my-property/my-cluster/my-ns/my-topic/my-subscription
        // or
        // /ws/reader/persistent/my-property/my-cluster/my-ns/my-topic
        checkArgument(parts.size() >= 8, "Invalid topic name format");
        checkArgument(parts.get(1).equals("ws"));
        checkArgument(parts.get(3).equals("persistent") || parts.get(3).equals("non-persistent"));

        DestinationName dn = DestinationName.get(parts.get(3), parts.get(4), parts.get(5), parts.get(6), parts.get(7));
        return dn.toString();
    }

    protected abstract Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception;

    private static final Logger log = LoggerFactory.getLogger(AbstractWebSocketHandler.class);
}
