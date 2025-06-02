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
package org.apache.pulsar.broker.authentication;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import javax.servlet.http.HttpServletRequest;
import org.testng.annotations.Test;

public class AuthenticationDataSubscriptionTest {

    AuthenticationDataSubscription target;

    @Test
    public void testTargetFromAuthenticationDataHttp(){
        var req = mock(HttpServletRequest.class);
        String headerName = "Authorization";
        String headerValue = "my-header";
        String authType = "my-authType";
        doReturn(headerValue).when(req).getHeader(eq(headerName));
        doReturn("localhost").when(req).getRemoteAddr();
        doReturn(4000).when(req).getRemotePort();
        doReturn(authType).when(req).getAuthType();
        AuthenticationDataSource authenticationDataSource = new AuthenticationDataHttp(req);
        target = new AuthenticationDataSubscription(authenticationDataSource, "my-sub");
        assertEquals(headerValue, target.getHttpHeader(headerName));
        assertEquals(authType, target.getHttpAuthType());
        assertEquals(true, target.hasDataFromHttp());
    }
}
