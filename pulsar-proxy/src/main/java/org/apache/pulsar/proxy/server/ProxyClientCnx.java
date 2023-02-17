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
package org.apache.pulsar.proxy.server;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.protocol.Commands;

@Slf4j
public class ProxyClientCnx extends ClientCnx {
    private final boolean forwardClientAuthData;
    private final String clientAuthMethod;
    private final String clientAuthRole;
    private final AuthData clientAuthData;
    private final ProxyConnection proxyConnection;

    public ProxyClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, String clientAuthRole,
                          AuthData clientAuthData, String clientAuthMethod, int protocolVersion,
                          boolean forwardClientAuthData, ProxyConnection proxyConnection) {
        super(conf, eventLoopGroup, protocolVersion);
        this.clientAuthRole = clientAuthRole;
        this.clientAuthData = clientAuthData;
        this.clientAuthMethod = clientAuthMethod;
        this.forwardClientAuthData = forwardClientAuthData;
        this.proxyConnection = proxyConnection;
    }

    @Override
    protected ByteBuf newConnectCommand() throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("New Connection opened via ProxyClientCnx with params clientAuthRole = {},"
                            + " clientAuthData = {}, clientAuthMethod = {}",
                    clientAuthRole, clientAuthData, clientAuthMethod);
        }

        authenticationDataProvider = authentication.getAuthData(remoteHostName);
        AuthData authData = authenticationDataProvider.authenticate(AuthData.INIT_AUTH_DATA);
        return Commands.newConnect(authentication.getAuthMethodName(), authData, protocolVersion,
                proxyConnection.clientVersion, proxyToTargetBrokerAddress, clientAuthRole, clientAuthData,
                clientAuthMethod);
    }

    @Override
    protected void handleAuthChallenge(CommandAuthChallenge authChallenge) {
        checkArgument(authChallenge.hasChallenge());
        checkArgument(authChallenge.getChallenge().hasAuthData());

        boolean isRefresh = Arrays.equals(AuthData.REFRESH_AUTH_DATA_BYTES, authChallenge.getChallenge().getAuthData());
        if (!forwardClientAuthData || !isRefresh) {
            super.handleAuthChallenge(authChallenge);
            return;
        }

        try {
            if (log.isDebugEnabled()) {
                log.debug("Proxy {} request to refresh the original client authentication data for "
                        + "the proxy client {}", proxyConnection.ctx().channel(), ctx.channel());
            }

            proxyConnection.ctx().writeAndFlush(Commands.newAuthChallenge(clientAuthMethod, AuthData.REFRESH_AUTH_DATA,
                            protocolVersion))
                    .addListener(writeFuture -> {
                        if (writeFuture.isSuccess()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Proxy {} sent the auth challenge to original client to refresh credentials "
                                                + "with method {} for the proxy client {}",
                                        proxyConnection.ctx().channel(), clientAuthMethod, ctx.channel());
                            }
                        } else {
                            log.error("Failed to send the auth challenge to original client by the proxy {} "
                                            + "for the proxy client {}",
                                    proxyConnection.ctx().channel(),
                                    ctx.channel(),
                                    writeFuture.cause());
                            closeWithException(writeFuture.cause());
                        }
                    });

            if (state == State.SentConnectFrame) {
                state = State.Connecting;
            }
        } catch (Exception e) {
            log.error("Failed to send the auth challenge to origin client by the proxy {} for the proxy client {}",
                    proxyConnection.ctx().channel(), ctx.channel(), e);
            closeWithException(e);
        }
    }
}
