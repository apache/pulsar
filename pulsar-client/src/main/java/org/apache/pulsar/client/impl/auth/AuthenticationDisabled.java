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
import java.util.Map;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;

public class AuthenticationDisabled implements Authentication, EncodedAuthenticationParameterSupport {

    protected final AuthenticationDataProvider nullData = new AuthenticationDataNull();
    public static final AuthenticationDisabled INSTANCE = new AuthenticationDisabled();
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public AuthenticationDisabled() {
    }

    @Override
    public String getAuthMethodName() {
        return "none";
    }

    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        return nullData;
    }

    @Override
    public void configure(String encodedAuthParamString) {
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
    }

    @Override
    public void start() throws PulsarClientException {
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }
}
