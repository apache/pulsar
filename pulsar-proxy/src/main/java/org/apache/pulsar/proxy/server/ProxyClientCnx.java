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
package org.apache.pulsar.proxy.server;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.BinaryAuthSession;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.netty.NettyChannelUtil;

@Slf4j
/**
 * Channel handler for Pulsar proxy's Pulsar broker client connections for lookup requests.
 * <p>
 * Please see {@link org.apache.pulsar.common.protocol.PulsarDecoder} javadoc for important details about handle*
 * method parameter instance lifecycle.
 */
public class ProxyClientCnx extends ClientCnx {
    private final boolean forwardClientAuthData;
    private String clientAuthMethod;
    private String clientAuthRole;

    private final ProxyConnection proxyConnection;
    private final BinaryAuthSession binaryAuthSession;

    public ProxyClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, int protocolVersion,
                          boolean forwardClientAuthData, ProxyConnection proxyConnection) {
        super(InstrumentProvider.NOOP, conf, eventLoopGroup, protocolVersion);
        this.forwardClientAuthData = forwardClientAuthData;
        this.proxyConnection = proxyConnection;
        this.binaryAuthSession = proxyConnection.getBinaryAuthSession();
    }

    @Override
    protected ByteBuf newConnectCommand() throws Exception {
        AuthData clientAuthData = null;
        if (binaryAuthSession != null) {
            clientAuthRole = binaryAuthSession.getAuthRole();
            clientAuthMethod = binaryAuthSession.getAuthMethod();
            if (forwardClientAuthData) {
                // There is a chance this auth data is expired because the ProxyConnection does not do early token
                // refresh.
                // Based on the current design, the best option is to configure the broker to accept slightly stale
                // authentication data.
                clientAuthData = AuthData.of(
                        binaryAuthSession.getAuthenticationData().getCommandData().getBytes(StandardCharsets.UTF_8));
            }

            // If original principal is null, it means the client connects the broker via proxy, else the client
            // connects the proxy via proxy.
            if (binaryAuthSession.getOriginalPrincipal() != null) {
                clientAuthRole = binaryAuthSession.getOriginalPrincipal();
                clientAuthMethod = binaryAuthSession.getOriginalAuthMethod();
                AuthenticationDataSource originalAuthData = binaryAuthSession.getOriginalAuthData();
                if (forwardClientAuthData && originalAuthData != null) {
                    clientAuthData = AuthData.of(
                            originalAuthData.getCommandData().getBytes(StandardCharsets.UTF_8));
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("New Connection opened via ProxyClientCnx with params clientAuthRole = {},"
                            + " clientAuthData = {}, clientAuthMethod = {}",
                    clientAuthRole, clientAuthData, clientAuthMethod);
        }
        authenticationDataProvider = authentication.getAuthData(remoteHostName);
        AuthData authData = authenticationDataProvider.authenticate(AuthData.INIT_AUTH_DATA);
        return Commands.newConnect(authentication.getAuthMethodName(), authData, protocolVersion,
                proxyConnection.clientVersion, proxyToTargetBrokerAddress, clientAuthRole, clientAuthData,
                clientAuthMethod, PulsarVersion.getVersion(), null);
    }

    @Override
    protected void handleAuthChallenge(CommandAuthChallenge authChallenge) {
        checkArgument(authChallenge.hasChallenge());
        checkArgument(authChallenge.getChallenge().hasAuthData());

        boolean isRefresh = Arrays.equals(AuthData.REFRESH_AUTH_DATA_BYTES, authChallenge.getChallenge().getAuthData());
        if (forwardClientAuthData && isRefresh) {
            proxyConnection.getValidClientAuthData()
                    .thenApplyAsync(authData -> {
                        NettyChannelUtil.writeAndFlushWithVoidPromise(ctx,
                                Commands.newAuthResponse(clientAuthMethod, authData, this.protocolVersion,
                                        String.format("Pulsar-Java-v%s", PulsarVersion.getVersion())));
                        return null;
                        }, ctx.executor())
                    .exceptionally(ex -> {
                        log.warn("Failed to get valid client auth data. Closing connection.", ex);
                        ctx.close();
                        return null;
                    });
        } else {
            super.handleAuthChallenge(authChallenge);
        }
    }
}
