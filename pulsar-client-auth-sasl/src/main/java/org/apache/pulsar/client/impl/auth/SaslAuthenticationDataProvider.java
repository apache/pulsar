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
package org.apache.pulsar.client.impl.auth;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.naming.AuthenticationException;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.SaslConstants;

@Slf4j
public class SaslAuthenticationDataProvider implements AuthenticationDataProvider {
    private static final long serialVersionUID = 1L;

    private PulsarSaslClient pulsarSaslClient;

    public SaslAuthenticationDataProvider(PulsarSaslClient pulsarSaslClient) {
        this.pulsarSaslClient = pulsarSaslClient;
    }

    @Override
    public boolean hasDataFromCommand() {
        return true;
    }

    // create token that evaluated by client, and will send to server.
    @Override
    public AuthData authenticate(AuthData commandData) throws AuthenticationException {
        // init
        if (Arrays.equals(commandData.getBytes(), AuthData.INIT_AUTH_DATA_BYTES)) {
            if (pulsarSaslClient.hasInitialResponse()) {
                return pulsarSaslClient.evaluateChallenge(AuthData.of(new byte[0]));
            }
            return AuthData.of(new byte[0]);
        }

        return pulsarSaslClient.evaluateChallenge(commandData);
    }

    @Override
    public boolean hasDataForHttp() {
        return true;
    }

    @Override
    public Set<Entry<String, String>> getHttpHeaders() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put(SaslConstants.SASL_HEADER_TYPE, SaslConstants.SASL_TYPE_VALUE);
        return headers.entrySet();
    }

}
