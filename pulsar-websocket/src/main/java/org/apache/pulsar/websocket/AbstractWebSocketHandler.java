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
import java.util.concurrent.CompletableFuture;

import javax.naming.AuthenticationException;
import javax.servlet.http.HttpServletRequest;

import org.apache.pulsar.common.naming.DestinationName;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

public abstract class AbstractWebSocketHandler extends WebSocketAdapter implements Closeable {

    protected final WebSocketService service;
    protected final HttpServletRequest request;

    protected final String topic;
    protected final Map<String, String> queryParams;

    public AbstractWebSocketHandler(WebSocketService service, HttpServletRequest request) {
        this.service = service;
        this.request = request;
        this.topic = extractTopicName(request);

        this.queryParams = new TreeMap<>();
        request.getParameterMap().forEach((key, values) -> {
            queryParams.put(key, values[0]);
        });
    }

    @Override
    public void onWebSocketConnect(Session session) {
        super.onWebSocketConnect(session);
        log.info("[{}] New WebSocket session on topic {}", session.getRemoteAddress(), topic);

        String authRole = "<none>";
        if (service.isAuthenticationEnabled()) {
            try {
                authRole = service.getAuthenticationService().authenticateHttpRequest(request);
                log.info("[{}] Authenticated WebSocket client {} on topic {}", session.getRemoteAddress(), authRole,
                        topic);

            } catch (AuthenticationException e) {
                log.warn("[{}] Failed to authenticated WebSocket client {} on topic {}: {}",
                        session.getRemoteAddress(), authRole, topic, e.getMessage());
                close(WebSocketError.AuthenticationError);
                return;
            }
        }

        if (service.isAuthorizationEnabled()) {
            try {
                if (!isAuthorized(authRole)) {
                    log.warn("[{}] WebSocket Client [{}] is not authorized on topic {}", session.getRemoteAddress(), authRole,
                            topic);
                    close(WebSocketError.NotAuthorizedError);
                    return;
                }
            } catch (Exception e) {
                log.warn("[{}] Got an exception when authorizing WebSocket client {} on topic {} on: {}",
                        session.getRemoteAddress(), authRole, topic, e.getMessage());
                close(WebSocketError.UnknownError);
                return;
            }
        }
        createClient(session);
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        super.onWebSocketError(cause);
        log.info("[{}] WebSocket error on topic {} : {}", getSession().getRemoteAddress(), topic, cause.getMessage());
        try {
            close();
        } catch (IOException e) {
            log.error("Failed in closing producer for topic[{}] with error: [{}]: ", topic, e.getMessage());
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
        checkArgument(parts.size() >= 8, "Invalid topic name format");
        checkArgument(parts.get(1).equals("ws"));
        checkArgument(parts.get(3).equals("persistent"));

        DestinationName dn = DestinationName.get("persistent", parts.get(4), parts.get(5), parts.get(6), parts.get(7));
        return dn.toString();
    }

    protected abstract Boolean isAuthorized(String authRole) throws Exception;

    protected abstract void createClient(Session session);

    private static final Logger log = LoggerFactory.getLogger(AbstractWebSocketHandler.class);
}
