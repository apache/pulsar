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
package org.apache.pulsar.broker.authentication;

import java.net.SocketAddress;
import java.security.cert.Certificate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthenticationDataSubscription implements AuthenticationDataSource {
    private final AuthenticationDataSource authData;
    private final String subscription;

    public AuthenticationDataSubscription(AuthenticationDataSource authData, String subscription) {
        this.authData = authData;
        this.subscription = subscription;
    }

    @Override
    public boolean hasDataFromCommand() {
        return hasAuthData() && authData.hasDataFromCommand();
    }

    @Override
    public String getCommandData() {
        return hasAuthData() ? authData.getCommandData() : null;
    }

    @Override
    public boolean hasDataFromPeer() {
        return hasAuthData() && authData.hasDataFromPeer();
    }

    @Override
    public SocketAddress getPeerAddress() {
        return hasAuthData() ? authData.getPeerAddress() : null;
    }

    @Override
    public boolean hasDataFromTls() {
        return hasAuthData() && authData.hasDataFromTls();
    }

    @Override
    public Certificate[] getTlsCertificates() {
        return hasAuthData() ? authData.getTlsCertificates() : null;
    }

    @Override
    public boolean hasSubscription() {
        return this.subscription != null;
    }

    @Override
    public String getSubscription() {
        return subscription;
    }

    @Override
    public boolean hasDataFromHttp() {
        return hasAuthData() && authData.hasDataFromHttp();
    }

    @Override
    public String getHttpAuthType() {
        return hasAuthData() ? authData.getHttpAuthType() : null;
    }

    @Override
    public String getHttpHeader(String name) {
        return hasAuthData() ? authData.getHttpHeader(name) : null;
    }

    public AuthenticationDataSource getAuthData() {
        return authData;
    }

    private boolean hasAuthData() {
        return authData != null;
    }
}
