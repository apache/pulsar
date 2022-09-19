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
package org.apache.pulsar.client.impl.auth.oauth2;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.pulsar.client.api.Authentication;

/**
 * Builder for {@link AuthenticationOAuth2} class.
 */
public final class AuthenticationOAuth2Builder {

    /**
     * Create a builder instance.
     *
     * @return builder
     */
    public static AuthenticationOAuth2Builder builder() {
        return new AuthenticationOAuth2Builder();
    }

    private double earlyTokenRefreshPercent = AuthenticationOAuth2.EARLY_TOKEN_REFRESH_PERCENT_DEFAULT;
    private ScheduledThreadPoolExecutor scheduler;
    private ClientCredentialsConfiguration clientCredentialsConfiguration;

    /**
     * Set the {@link ClientCredentialsConfiguration} when using the OAuth2 client credentials flow.
     * @return builder
     */
    public AuthenticationOAuth2Builder setClientCredentialsConfiguration(
            ClientCredentialsConfiguration clientCredentialsConfiguration) {
        if (clientCredentialsConfiguration == null) {
            throw new IllegalArgumentException("ClientCredentialsConfiguration cannot be null.");
        }
        this.clientCredentialsConfiguration = clientCredentialsConfiguration;
        return this;
    }

    /**
     * @param earlyTokenRefreshPercent - The percent of the expires_in time when the client should start attempting
     *                                 to refresh the token. If greater than or equal to 1, it is disabled. See
     *                                 {@link AuthenticationOAuth2} for details.
     * @return builder
     */
    public AuthenticationOAuth2Builder setEarlyTokenRefreshPercent(double earlyTokenRefreshPercent) {
        if (earlyTokenRefreshPercent <= 0) {
            throw new IllegalArgumentException("EarlyTokenRefreshPercent must be greater than 0.");
        }
        this.earlyTokenRefreshPercent = earlyTokenRefreshPercent;
        return this;
    }

    /**
     * @param scheduler - The scheduler to use for background refreshes of the token. If null and the
     *                  {@link #earlyTokenRefreshPercent} is less than 1, the client will create an internal scheduler.
     *                  Otherwise, it will use the passed in scheduler. If the caller supplies a scheduler, the
     *                  {@link AuthenticationOAuth2} will not close it.
     * @return builder
     */
    public AuthenticationOAuth2Builder setEarlyTokenRefreshExecutor(ScheduledThreadPoolExecutor scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public Authentication build() {
        if (clientCredentialsConfiguration == null) {
            throw new IllegalArgumentException("ClientCredentialsConfiguration must be set.");
        }
        Flow flow = new ClientCredentialsFlow(clientCredentialsConfiguration);
        return new AuthenticationOAuth2(flow, earlyTokenRefreshPercent, scheduler);
    }
}