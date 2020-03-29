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
package org.apache.pulsar.discovery.service;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType.Redirect;

import java.util.concurrent.TimeUnit;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnect;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;

/**
 * Handles incoming discovery request from client and sends appropriate response back to client
 *
 */
public class ServerConnection extends PulsarHandler {

    private DiscoveryService service;
    private String authRole = null;
    private AuthenticationDataSource authenticationData = null;
    private State state;
    public static final String TLS_HANDLER = "tls";

    enum State {
        Start, Connected
    }

    public ServerConnection(DiscoveryService discoveryService) {
        super(0, TimeUnit.SECONDS); // discovery-service doesn't need to run keepAlive task
        this.service = discoveryService;
        this.state = State.Start;
    }

    /**
     * handles connect request and sends {@code State.Connected} ack to client
     */
    @Override
    protected void handleConnect(CommandConnect connect) {
        checkArgument(state == State.Start);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received CONNECT from {}", remoteAddress);
        }
        if(service.getConfiguration().isAuthenticationEnabled()) {
            try {
                String authMethod = "none";
                if (connect.hasAuthMethodName()) {
                    authMethod = connect.getAuthMethodName();
                } else if (connect.hasAuthMethod()) {
                    // Legacy client is passing enum
                    authMethod = connect.getAuthMethod().name().substring(10).toLowerCase();
                }
                String authData = connect.getAuthData().toStringUtf8();
                ChannelHandler sslHandler = ctx.channel().pipeline().get(TLS_HANDLER);
                SSLSession sslSession = null;
                if (sslHandler != null) {
                    sslSession = ((SslHandler) sslHandler).engine().getSession();
                }
                this.authenticationData = new AuthenticationDataCommand(authData, remoteAddress, sslSession);
                authRole = service.getAuthenticationService()
                        .authenticate(this.authenticationData, authMethod);
                LOG.info("[{}] Client successfully authenticated with {} role {}", remoteAddress, authMethod, authRole);
            } catch (AuthenticationException e) {
                String msg = "Unable to authenticate";
                LOG.warn("[{}] {}: {}", remoteAddress, msg, e.getMessage());
                ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, msg));
                close();
                return;
            }
        }
        ctx.writeAndFlush(Commands.newConnected(connect.getProtocolVersion()));
        state = State.Connected;
        remoteEndpointProtocolVersion = connect.getProtocolVersion();
    }

    @Override
    protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata partitionMetadata) {
        checkArgument(state == State.Connected);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received PartitionMetadataLookup from {}", remoteAddress);
        }
        sendPartitionMetadataResponse(partitionMetadata);
    }

    /**
     * handles discovery request from client ands sends next active broker address
     */
    @Override
    protected void handleLookup(CommandLookupTopic lookup) {
        checkArgument(state == State.Connected);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received Lookup from {}", remoteAddress);
        }
        sendLookupResponse(lookup.getRequestId());
    }

    private void close() {
        ctx.close();
    }

    private void sendLookupResponse(long requestId) {
        try {
            LoadManagerReport availableBroker = service.getDiscoveryProvider().nextBroker();
            ctx.writeAndFlush(Commands.newLookupResponse(availableBroker.getPulsarServiceUrl(),
                    availableBroker.getPulsarServiceUrlTls(), false, Redirect, requestId, false));
        } catch (PulsarServerException e) {
            LOG.warn("[{}] Failed to get next active broker {}", remoteAddress, e.getMessage(), e);
            ctx.writeAndFlush(
                    Commands.newLookupErrorResponse(ServerError.ServiceNotReady, e.getMessage(), requestId));
        }
    }

    private void sendPartitionMetadataResponse(CommandPartitionedTopicMetadata partitionMetadata) {
        final long requestId = partitionMetadata.getRequestId();
        TopicName topicName = TopicName.get(partitionMetadata.getTopic());

        service.getDiscoveryProvider()
                .getPartitionedTopicMetadata(service, topicName, authRole, authenticationData)
                .thenAccept(metadata -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Total number of partitions for topic {} is {}", authRole, topicName, metadata.partitions);
            }
            ctx.writeAndFlush(Commands.newPartitionMetadataResponse(metadata.partitions, requestId));
        }).exceptionally(ex -> {
            LOG.warn("[{}] Failed to get partitioned metadata for topic {} {}", remoteAddress, topicName, ex.getMessage(), ex);
            ctx.writeAndFlush(
                    Commands.newPartitionMetadataResponse(ServerError.ServiceNotReady, ex.getMessage(), requestId));
            return null;
        });
    }

    @Override
    protected boolean isHandshakeCompleted() {
        return state == State.Connected;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ServerConnection.class);

}
