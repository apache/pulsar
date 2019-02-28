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
 * It is basically holding the the authentication state.
 * It tell broker whether the authentication is completed or not,
 * if completed, what is the AuthRole is.
 */
public class OneStageAuthenticationState implements AuthenticationState {

    private final AuthenticationDataSource authenticationDataSource;
    private final AuthenticationProvider provider;

    public OneStageAuthenticationState(AuthenticationDataSource authenticationDataSource,
                                       AuthenticationProvider provider) {
        this.authenticationDataSource = authenticationDataSource;
        this.provider = provider;
    }

    @Override
    public String getAuthRole() throws AuthenticationException {
        return provider.authenticate(authenticationDataSource);
    }

    /**
     * Challenge passed in auth data and get response data.
     */
    @Override
    public AuthData authenticate(AuthData authData) {
        return null;
    }

    @Override
    public boolean isComplete() {
        return true;
    }
}
