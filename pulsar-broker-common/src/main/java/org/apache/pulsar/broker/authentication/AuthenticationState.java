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

package org.apache.pulsar.broker.authentication;

import javax.naming.AuthenticationException;

import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.util.FutureUtil;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for authentication state.
 *
 * It tell broker whether the authentication is completed or not,
 * if completed, what is the AuthRole is.
 */
public interface AuthenticationState {
    /**
     * After the authentication between client and broker completed,
     * get authentication role represent for the client.
     * It should throw exception if auth not complete.
     */
    String getAuthRole() throws AuthenticationException;

    /**
     * Challenge passed in auth data and get response data.
     * @deprecated use and implement {@link AuthenticationState#authenticateAsync(AuthData)} instead.
     */
    @Deprecated
    AuthData authenticate(AuthData authData) throws AuthenticationException;

    /**
     * Challenge passed in auth data. If authentication is complete after the execution of this method, return null.
     * Otherwise, return response data to be sent to the client.
     *
     * <p>Note: the implementation of {@link AuthenticationState#authenticate(AuthData)} converted a null result into a
     * zero length byte array when {@link AuthenticationState#isComplete()} returned false after authentication. In
     * order to simplify this interface, the determination of whether to send a challenge back to the client is only
     * based on the result of this method. In order to maintain backwards compatibility, the default implementation of
     * this method calls {@link AuthenticationState#isComplete()} and returns a result compliant with the new
     * paradigm.</p>
     */
    default CompletableFuture<AuthData> authenticateAsync(AuthData authData) {
        try {
            AuthData result = this.authenticate(authData);
            if (isComplete()) {
                return CompletableFuture.completedFuture(null);
            } else {
                return result != null
                        ? CompletableFuture.completedFuture(result)
                        : CompletableFuture.completedFuture(AuthData.of(new byte[0]));
            }
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
    }

    /**
     * Return AuthenticationDataSource.
     */
    AuthenticationDataSource getAuthDataSource();

    /**
     * Whether the authentication is completed or not.
     * @deprecated this method's logic is captured by the result of
     * {@link AuthenticationState#authenticateAsync(AuthData)}. When the result is a {@link CompletableFuture} with a
     * null result, authentication is complete. When the result is a {@link CompletableFuture} with a nonnull result,
     * authentication is incomplete and requires an auth challenge.
     */
    @Deprecated
    boolean isComplete();

    /**
     * Get AuthenticationState ID
     */
    default long getStateId() {
        return -1L;
    }

    /**
     * If the authentication state is expired, it will force the connection to be re-authenticated.
     */
    default boolean isExpired() {
        return false;
    }

    /**
     * If the authentication state supports refreshing and the credentials are expired,
     * the auth provider will call this method to initiate the refresh process.
     * <p>
     * The auth state here will return the broker side data that will be used to send
     * a challenge to the client.
     *
     * @return the {@link AuthData} for the broker challenge to client
     * @throws AuthenticationException
     */
    default AuthData refreshAuthentication() throws AuthenticationException {
        return AuthData.REFRESH_AUTH_DATA;
    }
}
