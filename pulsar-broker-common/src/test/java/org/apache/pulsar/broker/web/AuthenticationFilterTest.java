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
package org.apache.pulsar.broker.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import javax.naming.AuthenticationException;
import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.testng.annotations.Test;

public class AuthenticationFilterTest {

    @Test
    public void testDoFilterWithAuthenticationException() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationFilter filter = new AuthenticationFilter(authenticationService);

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        String errorMsg = "Specific authentication error";
        doThrow(new AuthenticationException(errorMsg))
                .when(authenticationService)
                .authenticateHttpRequest(any(HttpServletRequest.class), any(HttpServletResponse.class));

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, errorMsg);
    }

    @Test
    public void testDoFilterWithGenericException() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationFilter filter = new AuthenticationFilter(authenticationService);

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        String errorMsg = "Some internal error";
        doThrow(new RuntimeException(errorMsg))
                .when(authenticationService)
                .authenticateHttpRequest(any(HttpServletRequest.class), any(HttpServletResponse.class));

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, errorMsg);
    }

    @Test
    public void testDoFilterWithNullMessageGenericException() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationFilter filter = new AuthenticationFilter(authenticationService);

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        doThrow(new RuntimeException())
                .when(authenticationService)
                .authenticateHttpRequest(any(HttpServletRequest.class), any(HttpServletResponse.class));

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Authentication required");
    }

    @Test
    public void testDoFilterWithNullMessageAuthenticationException() throws Exception {
        AuthenticationService authenticationService = mock(AuthenticationService.class);
        AuthenticationFilter filter = new AuthenticationFilter(authenticationService);

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        doThrow(new AuthenticationException(null))
                .when(authenticationService)
                .authenticateHttpRequest(any(HttpServletRequest.class), any(HttpServletResponse.class));

        filter.doFilter(request, response, chain);

        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Authentication required");
    }
}
