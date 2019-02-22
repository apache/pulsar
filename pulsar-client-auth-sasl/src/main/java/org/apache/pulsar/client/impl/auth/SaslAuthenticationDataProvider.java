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

import java.io.IOException;
import java.nio.charset.Charset;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.sasl.SaslConstants;

@Slf4j
public class SaslAuthenticationDataProvider implements AuthenticationDataProvider {
    private static final long serialVersionUID = 1L;

    // Client side token data, that will passed to sasl server side.
    // use byte[] directly to avoid converting string and byte[] for protobuf
    private byte[] clientSideToken;
    private PulsarSaslClient pulsarSaslClient;

    public SaslAuthenticationDataProvider(PulsarSaslClient pulsarSaslClient) {
        this.pulsarSaslClient = pulsarSaslClient;
    }

    @Override
    public boolean hasDataFromCommand() {
        return true;
    }

    @Override
    public byte[] getCommandDataBytes() {
        return clientSideToken;
    }

    // create token that evaluated by client, and will send to server.
    @Override
    public void setCommandDataBytes(byte[] commandData) throws IOException {
        if (new String(commandData, Charset.forName("UTF-8")).equalsIgnoreCase(SaslConstants.INIT_PROVIDER_DATA)) {
            // init
            if (pulsarSaslClient.hasInitialResponse()) {
                clientSideToken = pulsarSaslClient.evaluateChallenge(new byte[0]);
            } else {
                clientSideToken = new byte[0];
            }
        } else {
            clientSideToken = pulsarSaslClient.evaluateChallenge(commandData);
        }
    }

    @Override
    public boolean isComplete() {
        return this.pulsarSaslClient.isComplete();
    }

}
