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

import javax.naming.AuthenticationException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.websocket.WebSocketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketWebResource {

    public static final String ATTRIBUTE_PROXY_SERVICE_NAME = "webProxyService";
    public static final String ADMIN_PATH = "/admin";

    @Context
    protected ServletContext servletContext;

    @Context
    protected HttpServletRequest httpRequest;

    @Context
    protected UriInfo uri;

    private WebSocketService socketService;
    
    private String clientId;

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
        if (clientId != null && service().getConfig().isAuthenticationEnabled()) {
            try {
                clientId = service().getAuthenticationService().authenticateHttpRequest(httpRequest);
            } catch (AuthenticationException e) {
                throw new RestException(Status.UNAUTHORIZED, "Failed to get clientId from request");
            }
        } else {
            throw new RestException(Status.UNAUTHORIZED, "Failed to get auth data from the request");
        }
        return clientId;
    }
    
    
    /**
     * Checks whether the user has Pulsar Super-User access to the system.
     *
     * @throws WebApplicationException
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
     * @return
     */
    protected boolean validateUserAccess(DestinationName topic) {
        try {
            validateSuperUserAccess();
            return true;
        } catch (Exception e) {
            try {
                return isAuthorized(topic);
            } catch (Exception ne) {
                throw new RestException(ne);
            }
        }
    }

    /**
     * Checks if user is authorized to produce/consume on a given topic
     * 
     * @param topic
     * @return
     * @throws Exception
     */
    protected boolean isAuthorized(DestinationName topic) throws Exception {
        if (service().isAuthorizationEnabled()) {
            String authRole = clientAppId();
            return service().getAuthorizationManager().canLookup(topic, authRole);
        }
        return true;
    }

    private static final Logger log = LoggerFactory.getLogger(WebSocketWebResource.class);
}
