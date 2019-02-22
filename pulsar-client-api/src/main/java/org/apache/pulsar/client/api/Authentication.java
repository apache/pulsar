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
import javax.naming.AuthenticationException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;

/**
 * Interface of authentication providers.
 */
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
     *
     * Get/Create an authentication data provider which provides the data that this client will be sent to the broker.
     * Some authentication method need to auth between each client channel. So it need the broker, who it will talk to.
     *
     * @param brokerHostName
     *          target broker host name
     *
     * @return The authentication data provider
     */
    default AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
        throw new UnsupportedAuthenticationException("Method not implemented!");
    }

    /**
     * Configure the authentication plugins with the supplied parameters
     *
     * @param authParams
     * @deprecated This method will be deleted on version 2.0, instead please use configure(String
     *             encodedAuthParamString) which is in EncodedAuthenticationParameterSupport for now and will be
     *             integrated into this interface.
     */
    @Deprecated
    void configure(Map<String, String> authParams);

    /**
     * Initialize the authentication provider
     */
    void start() throws PulsarClientException;
}
