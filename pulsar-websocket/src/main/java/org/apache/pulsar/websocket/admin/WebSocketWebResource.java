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
package org.apache.pulsar.websocket.admin;

import static org.apache.commons.lang3.StringUtils.isBlank;
import javax.naming.AuthenticationException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.websocket.WebSocketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketWebResource {

    public static final String ATTRIBUTE_PROXY_SERVICE_NAME = "webProxyService";
    public static final String ADMIN_PATH_V1 = "/admin";
    public static final String ADMIN_PATH_V2 = "/admin/v2";

    @Context
    protected ServletContext servletContext;

    @Context
    protected HttpServletRequest httpRequest;

    @Context
    protected UriInfo uri;

    private WebSocketService socketService;

    private String clientId;
    private AuthenticationDataHttps authData;

    protected WebSocketService service() {
        if (socketService == null) {
            socketService = (WebSocketService) servletContext.getAttribute(ATTRIBUTE_PROXY_SERVICE_NAME);
        }
        return socketService;
    }

    /**
     * Gets a caller id (IP + role)
     *
     * @return the web service caller identification
     */
    public String clientAppId() {
        if (isBlank(clientId)) {
            try {
                clientId = service().getAuthenticationService().authenticateHttpRequest(httpRequest);
            } catch (AuthenticationException e) {
                if (service().getConfig().isAuthenticationEnabled()) {
                    throw new RestException(Status.UNAUTHORIZED, "Failed to get clientId from request");
                }
            }

            if (isBlank(clientId) && service().getConfig().isAuthenticationEnabled()) {
                throw new RestException(Status.UNAUTHORIZED, "Failed to get auth data from the request");
            }
        }
        return clientId;
    }

    public AuthenticationDataHttps authData() {
        if (authData == null) {
            authData = new AuthenticationDataHttps(httpRequest);
        }
        return authData;
    }

    /**
     * Checks whether the user has Pulsar Super-User access to the system.
     *
     * @throws RestException
     *             if not authorized
     */
    protected void validateSuperUserAccess() {
        if (service().getConfig().isAuthenticationEnabled()) {
            String appId = clientAppId();
            if (log.isDebugEnabled()) {
                log.debug("[{}] Check super user access: Authenticated: {} -- Role: {}", uri.getRequestUri(),
                        clientAppId(), appId);
            }
            if (!service().getConfig().getSuperUserRoles().contains(appId)) {
                throw new RestException(Status.UNAUTHORIZED, "This operation requires super-user access");
            }
        }
    }

    /**
     * Checks if user has super-user access or user is authorized to produce/consume on a given topic
     *
     * @param topic
     * @throws RestException
     */
    protected void validateUserAccess(TopicName topic) {
        boolean isAuthorized = false;

        try {
            validateSuperUserAccess();
            isAuthorized = true;
        } catch (Exception e) {
            try {
                isAuthorized = isAuthorized(topic);
            } catch (Exception ne) {
                throw new RestException(ne);
            }
        }

        if (!isAuthorized) {
            throw new RestException(Status.UNAUTHORIZED, "Don't have permission to access this topic");
        }
    }

    /**
     * Checks if user is authorized to produce/consume on a given topic
     *
     * @param topic
     * @return
     * @throws Exception
     */
    protected boolean isAuthorized(TopicName topic) throws Exception {
        if (service().isAuthorizationEnabled()) {
            return service().getAuthorizationService().canLookup(topic, clientAppId(), authData());
        }
        return true;
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketWebResource.class);
}
