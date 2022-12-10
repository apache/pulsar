/*
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
package org.apache.pulsar.websocket.proxy;

import java.io.IOException;

import javax.naming.AuthenticationException;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;

public class MockAuthenticationProvider implements AuthenticationProvider {

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {

    }

    @Override
    public String getAuthMethodName() {
        // method name
        return "mockauth";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        // Return super user role
        return "pulsar.super_user";
    }

}
