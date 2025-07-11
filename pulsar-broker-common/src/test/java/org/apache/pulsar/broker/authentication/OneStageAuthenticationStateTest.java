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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;
import javax.naming.AuthenticationException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;

public class OneStageAuthenticationStateTest {

    public static class CountingAuthenticationProvider implements AuthenticationProvider {
        public LongAdder authCallCount = new LongAdder();

        @Override
        public void initialize(ServiceConfiguration config) throws IOException {
        }

        @Override
        public String getAuthMethodName() {
            return null;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public CompletableFuture<String> authenticateAsync(AuthenticationDataSource authData) {
            authCallCount.increment();
            return CompletableFuture.completedFuture(authData.getCommandData());
        }

        public int getAuthCallCount() {
                return authCallCount.intValue();
        }
    }

    @Test
    public void verifyAuthenticateAsyncIsCalledExactlyOnceAndSetsRole() throws Exception {
        CountingAuthenticationProvider provider = new CountingAuthenticationProvider();
        AuthData authData = AuthData.of("role".getBytes());
        OneStageAuthenticationState authState = new OneStageAuthenticationState(authData, null, null, provider);
        assertEquals(provider.getAuthCallCount(), 0, "Auth count should not increase yet");
        AuthData challenge = authState.authenticateAsync(authData).get();
        assertNull(challenge);
        assertEquals(provider.getAuthCallCount(), 1, "Call authenticate only once");
        assertEquals(authState.getAuthRole(), "role");
        AuthenticationDataSource firstAuthenticationDataSource = authState.getAuthDataSource();
        assertTrue(firstAuthenticationDataSource instanceof AuthenticationDataCommand);

        // Verify subsequent call to authenticate does not change data
        AuthData secondChallenge = authState.authenticateAsync(AuthData.of("admin".getBytes())).get();
        assertNull(secondChallenge);
        assertEquals(authState.getAuthRole(), "role");
        AuthenticationDataSource secondAuthenticationDataSource = authState.getAuthDataSource();
        assertSame(secondAuthenticationDataSource, firstAuthenticationDataSource);
        assertEquals(provider.getAuthCallCount(), 1, "Call authenticate only once, even later.");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void verifyAuthenticateIsCalledExactlyOnceAndSetsRole() throws Exception {
        CountingAuthenticationProvider provider = new CountingAuthenticationProvider();
        AuthData authData = AuthData.of("role".getBytes());
        OneStageAuthenticationState authState = new OneStageAuthenticationState(authData, null, null, provider);
        assertEquals(provider.getAuthCallCount(), 0, "Auth count should not increase yet");
        assertFalse(authState.isComplete());
        AuthData challenge = authState.authenticate(authData);
        assertNull(challenge);
        assertTrue(authState.isComplete());
        assertEquals(provider.getAuthCallCount(), 1, "Call authenticate only once");
        assertEquals(authState.getAuthRole(), "role");
        AuthenticationDataSource firstAuthenticationDataSource = authState.getAuthDataSource();
        assertTrue(firstAuthenticationDataSource instanceof AuthenticationDataCommand);

        // Verify subsequent call to authenticate does not change data
        AuthData secondChallenge = authState.authenticate(AuthData.of("admin".getBytes()));
        assertNull(secondChallenge);
        assertEquals(authState.getAuthRole(), "role");
        AuthenticationDataSource secondAuthenticationDataSource = authState.getAuthDataSource();
        assertSame(secondAuthenticationDataSource, firstAuthenticationDataSource);
        assertEquals(provider.getAuthCallCount(), 1, "Call authenticate only once, even later.");
    }

    @Test
    public void verifyGetAuthRoleBeforeAuthenticateFails() {
        CountingAuthenticationProvider provider = new CountingAuthenticationProvider();
        AuthData authData = AuthData.of("role".getBytes());
        OneStageAuthenticationState authState = new OneStageAuthenticationState(authData, null, null, provider);
        assertThrows(AuthenticationException.class, authState::getAuthRole);
        assertNull(authState.getAuthDataSource());
    }

    @Test
    public void verifyHttpAuthConstructorInitializesAuthDataSourceAndDoesNotAuthenticateData() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("localhost");
        when(request.getRemotePort()).thenReturn(8080);
        CountingAuthenticationProvider provider = new CountingAuthenticationProvider();
        OneStageAuthenticationState authState = new OneStageAuthenticationState(request, provider);
        assertNotNull(authState.getAuthDataSource());
        assertEquals(provider.getAuthCallCount(), 0);
    }
}
