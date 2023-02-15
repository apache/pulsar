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
package org.apache.pulsar.broker.auth;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import java.net.SocketAddress;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;

/**
 * Class that provides the same authentication semantics as the {@link MockAuthenticationProvider} except
 * that this one initializes the {@link MockMultiStageAuthenticationState} class to support testing
 * multistage authentication.
 */
public class MockMultiStageAuthenticationProvider extends MockAuthenticationProvider {

    @Override
    public String getAuthMethodName() {
        return "multi-stage";
    }

    @Override
    public AuthenticationState newAuthState(AuthData authData,
                                            SocketAddress remoteAddress,
                                            SSLSession sslSession) throws AuthenticationException {
        return new MockMultiStageAuthenticationState(this);
    }
}
