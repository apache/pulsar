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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.AuthData;

@Slf4j
public class SaslAuthenticationDataSource implements AuthenticationDataSource {
    private static final long serialVersionUID = 1L;

    // server side token data, that will passed to sasl client side.
    protected AuthData serverSideToken;
    private PulsarSaslServer pulsarSaslServer;

    public SaslAuthenticationDataSource(PulsarSaslServer saslServer) {
        this.pulsarSaslServer = saslServer;
    }

    @Override
    public boolean hasDataFromCommand() {
        return true;
    }

    @Override
    public AuthData authenticate(AuthData data) throws AuthenticationException {
        serverSideToken = pulsarSaslServer.response(data);
        return serverSideToken;
    }

    public boolean isComplete() {
        return this.pulsarSaslServer.isComplete();
    }

    public String getAuthorizationID() {
        return pulsarSaslServer.getAuthorizationID();
    }
}
