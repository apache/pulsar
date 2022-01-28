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
package org.apache.pulsar.client.api;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Interface of authentication providers.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface Authentication extends Closeable, Serializable {

    /**
     * @return the identifier for this authentication method
     */
    String getAuthMethodName();

    /**
     *
     * @return The authentication data identifying this client that will be sent to the broker
     * @throws PulsarClientException.GettingAuthenticationDataException
     *             if there was error getting the authentication data to use
     * @throws PulsarClientException
     *             any other error
     */
    default AuthenticationDataProvider getAuthData() throws PulsarClientException {
        throw new UnsupportedAuthenticationException("Method not implemented!");
    }

    /**
     * Get/Create an authentication data provider which provides the data that this client will be sent to the broker.
     * Some authentication method need to auth between each client channel. So it need the broker, who it will talk to.
     *
     * @param brokerHostName
     *          target broker host name
     *
     * @return The authentication data provider
     */
    default AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
        return this.getAuthData();
    }

    /**
     * Configure the authentication plugins with the supplied parameters.
     *
     * @param authParams
     * @deprecated This method will be deleted on version 2.0, instead please use configure(String
     *             encodedAuthParamString) which is in EncodedAuthenticationParameterSupport for now and will be
     *             integrated into this interface.
     */
    @Deprecated
    void configure(Map<String, String> authParams);

    /**
     * Initialize the authentication provider.
     */
    void start() throws PulsarClientException;

    /**
     * An authentication Stage.
     * when authentication complete, passed-in authFuture will contains authentication related http request headers.
     */
    default void authenticationStage(String requestUrl,
                                     AuthenticationDataProvider authData,
                                     Map<String, String> previousResHeaders,
                                     CompletableFuture<Map<String, String>> authFuture) {
        authFuture.complete(null);
    }

    /**
     * Add an authenticationStage that will complete along with authFuture.
     */
    default Set<Entry<String, String>> newRequestHeader(String hostName,
                                                        AuthenticationDataProvider authData,
                                                        Map<String, String> previousResHeaders) throws Exception {
        return authData.getHttpHeaders();
    }

}
