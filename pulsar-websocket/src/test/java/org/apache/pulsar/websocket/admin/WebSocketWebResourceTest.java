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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import javax.naming.AuthenticationException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import com.google.common.collect.Sets;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.websocket.WebSocketService;

public class WebSocketWebResourceTest {

    private static final String SUPER_USER = "super";
    private static final String AUTHORIZED_USER = "authorized";
    private static final String UNAUTHORIZED_USER = "unauthorized";

    private TopicName topicName;

    @InjectMocks
    @Spy
    private WebSocketWebResource webResource;
    @Mock
    private ServletContext servletContext;
    @Mock
    private HttpServletRequest httpRequest;
    @Mock
    private UriInfo uri;

    @BeforeMethod
    public void setup(Method method) throws Exception {
        MockitoAnnotations.initMocks(this);

        ServiceConfiguration config = new ServiceConfiguration();
        config.setSuperUserRoles(Sets.newHashSet(SUPER_USER));
        if ("testAuthenticationDisabled".equals(method.getName())) {
            config.setAuthenticationEnabled(false);
            config.setAuthorizationEnabled(false);
        } else {
            config.setAuthenticationEnabled(true);
            config.setAuthorizationEnabled(true);
        }

        AuthenticationService authnService = mock(AuthenticationService.class);
        if ("testSuperUserAccess".equals(method.getName())) {
            when(authnService.authenticateHttpRequest(any(HttpServletRequest.class))).thenReturn(SUPER_USER);
        } else if ("testUnauthorizedUserAccess".equals(method.getName())) {
            when(authnService.authenticateHttpRequest(any(HttpServletRequest.class))).thenReturn(UNAUTHORIZED_USER);
        } else if ("testBlankUserAccess".equals(method.getName())) {
            when(authnService.authenticateHttpRequest(any(HttpServletRequest.class))).thenReturn("");
        } else if ("testUnauthenticatedUserAccess".equals(method.getName())) {
            when(authnService.authenticateHttpRequest(any(HttpServletRequest.class)))
                    .thenThrow(new AuthenticationException());
        } else {
            when(authnService.authenticateHttpRequest(any(HttpServletRequest.class))).thenReturn(AUTHORIZED_USER);
        }

        AuthorizationService authzService = mock(AuthorizationService.class);
        when(authzService.canLookup(any(TopicName.class), eq(SUPER_USER), any(AuthenticationDataSource.class)))
                .thenReturn(true);
        when(authzService.canLookup(any(TopicName.class), eq(AUTHORIZED_USER), any(AuthenticationDataSource.class)))
                .thenReturn(true);
        when(authzService.canLookup(any(TopicName.class), eq(UNAUTHORIZED_USER), any(AuthenticationDataSource.class)))
                .thenReturn(false);
        when(authzService.canLookup(any(TopicName.class), eq(""), any(AuthenticationDataSource.class)))
                .thenReturn(false);

        WebSocketService socketService = mock(WebSocketService.class);
        when(socketService.getConfig()).thenReturn(config);
        when(socketService.isAuthorizationEnabled()).thenReturn(config.isAuthorizationEnabled());
        when(socketService.getAuthenticationService()).thenReturn(authnService);
        when(socketService.getAuthorizationService()).thenReturn(authzService);

        // Mock WebSocketWebResource
        doReturn(mock(AuthenticationDataHttps.class)).when(webResource).authData();

        // Mock ServletContext
        when(servletContext.getAttribute(anyString())).thenReturn(socketService);

        // Mock UriInfo
        when(uri.getRequestUri()).thenReturn(null);

        topicName = TopicName.get("persistent://tenant/cluster/ns/dest");
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        this.webResource = null;
    }

    @Test
    public void testAuthenticationDisabled() throws Exception {
        try {
            Assert.assertEquals(webResource.clientAppId(), AUTHORIZED_USER);
            webResource.validateSuperUserAccess();
            webResource.validateUserAccess(topicName);
        } catch (RestException e) {
            Assert.fail("Should not fail", e);
        }
    }

    @Test
    public void testSuperUserAccess() throws Exception {
        try {
            Assert.assertEquals(webResource.clientAppId(), SUPER_USER);
            webResource.validateSuperUserAccess();
            webResource.validateUserAccess(topicName);
        } catch (RestException e) {
            Assert.fail("Should not fail", e);
        }
    }

    @Test
    public void testAuthorizedUserAccess() throws Exception {
        try {
            webResource.validateSuperUserAccess();
            Assert.fail("Should fail");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }

        try {
            Assert.assertEquals(webResource.clientAppId(), AUTHORIZED_USER);
            webResource.validateUserAccess(topicName);
        } catch (RestException e) {
            Assert.fail("Should not fail", e);
        }
    }

    @Test
    public void testUnauthorizedUserAccess() throws Exception {
        try {
            Assert.assertEquals(webResource.clientAppId(), UNAUTHORIZED_USER);
        } catch (RestException e) {
            Assert.fail("Should not fail", e);
        }

        try {
            webResource.validateSuperUserAccess();
            Assert.fail("Should fail");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }

        try {
            webResource.validateUserAccess(topicName);
            Assert.fail("Should fail");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }
    }

    @Test
    public void testBlankUserAccess() throws Exception {
        try {
            webResource.clientAppId();
            Assert.fail("Should fail");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }

        try {
            webResource.validateSuperUserAccess();
            Assert.fail("Should fail");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }

        try {
            webResource.validateUserAccess(topicName);
            Assert.fail("Should fail");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }
    }

    @Test
    public void testUnauthenticatedUserAccess() throws Exception {
        try {
            webResource.clientAppId();
            Assert.fail("Should fail");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }

        try {
            webResource.validateSuperUserAccess();
            Assert.fail("Should fail");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }

        try {
            webResource.validateUserAccess(topicName);
            Assert.fail("Should fail");
        } catch (RestException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Status.UNAUTHORIZED.getStatusCode());
        }
    }

}
