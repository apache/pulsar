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
     */
    AuthData authenticate(AuthData authData) throws AuthenticationException;

    /**
     * Return AuthenticationDataSource.
     */
    AuthenticationDataSource getAuthDataSource();

    /**
     * Whether the authentication is completed or not.
     */
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
