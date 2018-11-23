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
package org.apache.pulsar.grpc;

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.util.Map;

public class AuthenticationDataGrpc implements AuthenticationDataSource {

    protected final SocketAddress remoteAddress;
    protected final SSLSession sslSession;
    protected final Map<String, String> authHeaders;

    public AuthenticationDataGrpc(SocketAddress remoteAddress, SSLSession sslSession, Map<String, String> authHeaders) {
        this.remoteAddress = remoteAddress;
        this.sslSession = sslSession;
        this.authHeaders = authHeaders;
    }


    @Override
    public boolean hasDataFromTls() {
        return (sslSession != null);
    }

    @Override
    public Certificate[] getTlsCertificates() {
        try {
            return sslSession.getPeerCertificates();
        } catch (SSLPeerUnverifiedException e) {
            return null;
        }
    }

    @Override
    public boolean hasDataFromHttp() {
        return true;
    }

    @Override
    public String getHttpHeader(String name) {
        return authHeaders.get(name);
    }

    @Override
    public boolean hasDataFromPeer() {
        return remoteAddress != null;
    }

    @Override
    public SocketAddress getPeerAddress() {
        return remoteAddress;
    }
}
