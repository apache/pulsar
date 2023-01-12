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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;
import javax.naming.AuthenticationException;

// Suppress the warnings, these tests verify the behavior of deprecated methods
@SuppressWarnings("deprecation")
public class OneStageAuthenticationStateTest {

    @Test
    public void verifyAuthenticateIsCalledExactlyOnce() throws Exception {
        AuthenticationProvider provider = mock(AuthenticationProvider.class);
        OneStageAuthenticationState authState = new OneStageAuthenticationState(null, null, provider);
        authState.authenticate(AuthData.of("data".getBytes()));
        verify(provider).authenticate(any());
    }

    @Test
    public void verifyAuthenticateIsCalledExactlyTwice() throws Exception {
        AuthenticationProvider provider = mock(AuthenticationProvider.class);
        AuthData authData = AuthData.of("data".getBytes());
        OneStageAuthenticationState authState = new OneStageAuthenticationState(authData, null, null, provider);
        authState.authenticate(authData);
        verify(provider, times(2)).authenticate(any());
    }

    @Test
    public void verifyAuthenticateSetsAuthRole() throws Exception {
        String role = "my-role";
        AuthenticationProvider provider = mock(AuthenticationProvider.class);
        when(provider.authenticate(any())).thenReturn(role);
        OneStageAuthenticationState authState = new OneStageAuthenticationState(null, null, provider);
        authState.authenticate(AuthData.INIT_AUTH_DATA);
        assertEquals(authState.getAuthRole(), role, "Expect roles to match");
        assertTrue(authState.getAuthDataSource() instanceof AuthenticationDataCommand);
    }

    @Test
    public void verifyClassInitializationSetsAuthRole() throws Exception {
        String role = "my-role";
        AuthenticationProvider provider = mock(AuthenticationProvider.class);
        AuthData authData = AuthData.of("data".getBytes());
        when(provider.authenticate(any())).thenReturn(role);
        OneStageAuthenticationState authState = new OneStageAuthenticationState(authData, null, null, provider);
        assertEquals(authState.getAuthRole(), role, "Expect roles to match");
        assertTrue(authState.getAuthDataSource() instanceof AuthenticationDataCommand);
    }

    @Test
    public void verifyGetAuthRoleFailsIfCalledBeforeAuthenticate() {
        AuthenticationProvider provider = mock(AuthenticationProvider.class);
        OneStageAuthenticationState authState = new OneStageAuthenticationState(null, null, provider);
        assertThrows(AuthenticationException.class, authState::getAuthRole);
        assertNull(authState.getAuthDataSource());
    }

}
