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

import io.grpc.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import java.net.SocketAddress;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.pulsar.grpc.Constants.*;

public class AuthenticationServerInterceptor implements ServerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationServerInterceptor.class);

    private final AuthenticationService authenticationService;

    public AuthenticationServerInterceptor(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
        Context ctx = Context.current();

        SocketAddress socketAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        try {
            Map<String, String> authHeaders = Stream.of(AUTHORIZATION_METADATA_KEY, ATHENZ_METADATA_KEY)
                    .map(key -> Pair.of(key.originalName(), metadata.get(key)))
                    .filter(pair -> pair.getValue() != null)
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            SSLSession sslSession = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
            AuthenticationDataSource authenticationData = new AuthenticationDataGrpc(socketAddress, sslSession, authHeaders);
            String role = authenticationService.authenticateWithAnyProvider(authenticationData);
            ctx = ctx
                    .withValue(AUTHENTICATION_ROLE_CTX_KEY, role)
                    .withValue(AUTHENTICATION_DATA_CTX_KEY, authenticationData);
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Authenticated HTTP request with role {}", socketAddress, role);
            }
        } catch (AuthenticationException e) {
            LOG.warn("[{}] Failed to authenticate HTTP request: {}", socketAddress, e.getMessage());
            throw Status.UNAUTHENTICATED.withDescription(e.getMessage()).asRuntimeException();
        }

        return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
    }
}
