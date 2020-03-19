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
package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.*;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.protocols.grpc.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.pulsar.protocols.grpc.Constants.*;

public class AuthenticationInterceptor implements ServerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationInterceptor.class);

    private final BrokerService service;
    private final HmacSigner signer;
    private Map<String, Map<Long, AuthenticationState>> authStates = new ConcurrentHashMap<>();

    private static final long SASL_ROLE_TOKEN_LIVE_SECONDS = 3600;
    // A signer for role token, with random secret.

    public AuthenticationInterceptor(BrokerService service) {
        this(service, new HmacSigner());
    }

    public AuthenticationInterceptor(BrokerService service, HmacSigner signer) {
        this.service = service;
        this.signer = signer;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
        Context ctx = Context.current();

        if (service.isAuthenticationEnabled()) {
            SocketAddress remoteAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
            SSLSession sslSession = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);

            try {
                if (metadata.containsKey(AUTH_ROLE_TOKEN_METADATA_KEY)) {
                    AuthRoleToken token = AuthRoleToken.parseFrom(metadata.get(AUTH_ROLE_TOKEN_METADATA_KEY));
                    byte[] signature = signer.computeSignature(token.getRoleInfo().toByteArray());
                    if (!Arrays.equals(signature, token.getSignature().toByteArray())) {
                        throw new AuthenticationException("Invalid signature");
                    }
                    long expires = token.getRoleInfo().getExpires();
                    if (expires != -1 && expires < System.currentTimeMillis()) {
                        // role token expired, send role token expired to client.
                        throw new AuthenticationException("Role token expired");
                    }
                    // role token OK to use
                    ctx = ctx.withValue(AUTH_ROLE_CTX_KEY, token.getRoleInfo().getRole());
                    ctx = ctx.withValue(AUTH_DATA_CTX_KEY, new AuthenticationDataCommand(null, remoteAddress, sslSession));
                } else if (metadata.containsKey(AUTH_METADATA_KEY)) {
                    CommandAuth auth = CommandAuth.parseFrom(metadata.get(AUTH_METADATA_KEY));
                    AuthData clientData = AuthData.of(auth.getAuthData().toByteArray());
                    String authMethod = auth.hasAuthMethod() ?  auth.getAuthMethod() : "none";
                    AuthenticationProvider authenticationProvider = service.getAuthenticationService()
                            .getAuthenticationProvider(authMethod);

                    // Not find provider named authMethod. Most used for tests.
                    // In AuthenticationDisabled, it will set authMethod "none".
                    if (authenticationProvider == null) {
                        String authRole = service.getAuthenticationService().getAnonymousUserRole()
                                .orElseThrow(() ->
                                        new AuthenticationException("No anonymous role, and no authentication provider configured"));
                        ctx = ctx.withValue(AUTH_ROLE_CTX_KEY, authRole);
                    } else {
                        AuthenticationState authState = authenticationProvider.newAuthState(clientData, remoteAddress, sslSession);
                        AuthData brokerData = authState.authenticate(clientData);
                        if (authState.isComplete()) {
                            String authRole = authState.getAuthRole();
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Client successfully authenticated with {} role {}",
                                        remoteAddress, authMethod, authRole);
                            }
                            ctx = ctx.withValue(AUTH_ROLE_CTX_KEY, authRole);
                            ctx = ctx.withValue(AUTH_DATA_CTX_KEY, authState.getAuthDataSource());
                        } else {
                            Map<Long, AuthenticationState> authMethodStates = authStates.putIfAbsent(authMethod, new ConcurrentHashMap<>());
                            if (authMethodStates == null) {
                                authMethodStates = authStates.get(authMethod);
                            }
                            authMethodStates.put(authState.getStateId(), authState);
                            CommandAuthChallenge challenge = Commands.newAuthChallenge(authMethod, brokerData, authState.getStateId());
                            return closeWithSingleHeader(serverCall, AUTHCHALLENGE_METADATA_KEY, challenge.toByteArray());
                        }
                    }
                } else if (metadata.containsKey(AUTHRESPONSE_METADATA_KEY)) {
                    CommandAuthResponse authResponse = CommandAuthResponse.parseFrom(metadata.get(AUTHRESPONSE_METADATA_KEY));
                    String authMethod = authResponse.getResponse().getAuthMethodName();
                    long authStateId = authResponse.getResponse().getAuthStateId();
                    AuthenticationState authState = null;
                    if (authStates.containsKey(authMethod)) {
                        authState = authStates.get(authMethod).get(authStateId);
                    }
                    if (authState == null) {
                        throw new AuthenticationException("Auth state id not found");
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Received AuthResponse from {}, auth method: {}",
                                remoteAddress, authMethod);
                    }

                    try {
                        AuthData clientData = AuthData.of(authResponse.getResponse().getAuthData().toByteArray());
                        AuthData brokerData = authState.authenticate(clientData);
                        if (authState.isComplete()) {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Client successfully authenticated with {} role {}",
                                        remoteAddress, authMethod, authState.getAuthRole());
                            }
                            String authRole = authState.getAuthRole();
                            AuthRoleToken authToken = createAuthRoleToken(authRole, authState.getStateId());

                            // auth completed, no need to keep authState
                            authStates.get(authMethod).remove(authState.getStateId());
                            return closeWithSingleHeader(serverCall, AUTH_ROLE_TOKEN_METADATA_KEY, authToken.toByteArray());
                        } else {
                            CommandAuthChallenge challenge = Commands.newAuthChallenge(authMethod, brokerData, authState.getStateId());
                            return closeWithSingleHeader(serverCall, AUTHCHALLENGE_METADATA_KEY, challenge.toByteArray());
                        }

                    } catch (Exception e) {
                        String msg = "Unable to handleAuthResponse";
                        log.warn("[{}] {} ", remoteAddress, msg, e);
                        throw new AuthenticationException(msg);
                    }
                } else {
                    throw new AuthenticationException("Authentication missing");
                }
            } catch (AuthenticationException | InvalidProtocolBufferException e) {
                serverCall.close(Status.UNAUTHENTICATED.withDescription(e.getMessage()), new Metadata());
                return new ServerCall.Listener<ReqT>() {};
            }

            // TODO: handle original principal
        }

        return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> closeWithSingleHeader(ServerCall<ReqT, RespT> serverCall,
            Metadata.Key<byte[]> key, byte[] bytes) {
        Metadata metadata = new Metadata();
        metadata.put(key, bytes);
        serverCall.close(Status.UNAUTHENTICATED, metadata);
        return new ServerCall.Listener<ReqT>() { };
    }

    private AuthRoleToken createAuthRoleToken(String role, long sessionId) {
        long expireAtMs = System.currentTimeMillis() + SASL_ROLE_TOKEN_LIVE_SECONDS * 1000; // 1 hour
        AuthRoleTokenInfo roleTokenInfo = AuthRoleTokenInfo.newBuilder()
                .setRole(role)
                .setSessionId(sessionId)
                .setExpires(expireAtMs)
                .build();

        byte[] signature = signer.computeSignature(roleTokenInfo.toByteArray());

        if (log.isDebugEnabled()) {
            log.debug("create role token role: {} session :{}, expires:{}",
                    role, sessionId, expireAtMs);
        }

        return AuthRoleToken.newBuilder()
                .setRoleInfo(roleTokenInfo)
                .setSignature(ByteString.copyFrom(signature))
                .build();
    }

}
