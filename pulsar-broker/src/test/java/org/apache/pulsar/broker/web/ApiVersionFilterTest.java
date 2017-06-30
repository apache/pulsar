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
package org.apache.pulsar.broker.web;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import java.util.Optional;

import javax.servlet.FilterChain;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.web.ApiVersionFilter;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.broker.web.VipStatus;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.mockito.Matchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ApiVersionFilterTest {

    private PulsarService pulsar;
    private ZooKeeperCache localCache;
    private HttpServletRequest req;
    private HttpServletResponse resp;
    private FilterChain chain;

    @BeforeMethod
    public void perTestSetup() {
        pulsar = mock(PulsarService.class);
        localCache = mock(ZooKeeperCache.class);
        req = mock(HttpServletRequest.class);
        resp = mock(HttpServletResponse.class);
        chain = mock(FilterChain.class);
    }

    /**
     * Test that if the client reports a valid version, the request is allowed.
     */
    @Test
    public void testClientWithValidVersion() throws Exception {
        ApiVersionFilter filter = new ApiVersionFilter(pulsar, false);

        // Set up mock behaviour for the test.
        when(pulsar.getLocalZkCache()).thenReturn(localCache);
        when(localCache.getData(eq("/minApiVersion"), Matchers.anyObject())).thenReturn(Optional.of("1.0"));
        when(req.getHeader("pulsar-client-version")).thenReturn("1.0");

        filter.doFilter(req, resp, chain);
        verify(chain).doFilter(req, resp);
    }

    /**
     * Test that if the client reports a version lower than the minApiVersion, the request is rejected.
     */
    @Test
    public void testClientWithLowVersion() throws Exception {
        ApiVersionFilter filter = new ApiVersionFilter(pulsar, false);

        // Set up mock behaviour for the test.
        when(pulsar.getLocalZkCache()).thenReturn(localCache);
        when(localCache.getData(eq("/minApiVersion"), Matchers.anyObject())).thenReturn(Optional.of("1.0"));
        when(req.getHeader("pulsar-client-version")).thenReturn("0.9");

        filter.doFilter(req, resp, chain);
        verify(resp).sendError(HttpServletResponse.SC_BAD_REQUEST, "Unsuported Client version");
        verify(chain, never()).doFilter(req, resp);
    }

    /**
     * Test that if the min client version cannot be determined from ZooKeeper, the request is allowed.
     */
    @Test
    public void testZKReadFailureAllowsAccess() throws Exception {
        ApiVersionFilter filter = new ApiVersionFilter(pulsar, false);

        // Set up mock behaviour for the test.
        when(pulsar.getLocalZkCache()).thenReturn(localCache);
        doThrow(new RuntimeException()).when(localCache).getData(eq("/minApiVersion"), Matchers.anyObject());
        when(req.getHeader("pulsar-client-version")).thenReturn("0.9");

        filter.doFilter(req, resp, chain);
        verify(chain).doFilter(req, resp);
    }

    /**
     * Test that if the either the min client version or the reported version is incorrectly formatted, the request is
     * allowed.
     */
    @Test
    public void testBadVersionFormatRequestIsAllowed() throws Exception {
        ApiVersionFilter filter = new ApiVersionFilter(pulsar, false);

        // Set up mock behaviour for the test.
        when(pulsar.getLocalZkCache()).thenReturn(localCache);
        when(localCache.getData(eq("/minApiVersion"), Matchers.anyObject())).thenReturn(Optional.of("foo"));
        when(req.getHeader("pulsar-client-version")).thenReturn("0.9");

        filter.doFilter(req, resp, chain);
        verify(chain).doFilter(req, resp);
    }

    /**
     * Test that if the client version is not reported, the allowUnversionedClients field is used to determine the
     * response.
     */
    @Test
    public void testDefaultVersionAssumedOnUnreportedVersion() throws Exception {
        ApiVersionFilter filter1 = new ApiVersionFilter(pulsar, true);
        ApiVersionFilter filter2 = new ApiVersionFilter(pulsar, false);

        // Set up mock behaviour for the test.
        when(pulsar.getLocalZkCache()).thenReturn(localCache);
        when(localCache.getData(eq("/minApiVersion"), Matchers.anyObject())).thenReturn(Optional.of("1.0"));

        // Returning null here will simulate the client not sending a version,
        // which should cause the response to be determined by the
        // allowUnversionedClients field.
        when(req.getHeader("pulsar-client-version")).thenReturn(null);

        filter1.doFilter(req, resp, chain);
        filter2.doFilter(req, resp, chain);
        verify(chain).doFilter(req, resp);
    }

    @Test
    public void testVipStatus() {
        VipStatus vipStatus = spy(new VipStatus());
        vipStatus.setPulsar(pulsar);
        when(pulsar.getStatusFilePath()).thenReturn("testFile.html");
        try {
            vipStatus.checkStatus();
            fail();
        } catch (WebApplicationException e) {
            // Ok
        }
    }

    @Test
    public void testAuthFilterTest() throws Exception {
        BrokerService nativeService = mock(BrokerService.class);
        AuthenticationService authService = mock(AuthenticationService.class);
        doReturn(nativeService).when(pulsar).getBrokerService();
        doReturn(authService).when(nativeService).getAuthenticationService();
        doReturn("test-role").when(authService).authenticateHttpRequest(mock(HttpServletRequest.class));
        AuthenticationFilter filter = new AuthenticationFilter(pulsar);
        HttpServletRequest request = mock(HttpServletRequest.class);
        ServletResponse response = null;
        doNothing().when(request).setAttribute(anyString(), anyObject());
        filter.doFilter(request, response, chain);
    }
}
