/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.discovery.service;

import static com.google.common.base.Preconditions.checkArgument;
import static com.yahoo.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType.Redirect;

import java.util.concurrent.TimeUnit;

import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.authentication.AuthenticationDataCommand;
import com.yahoo.pulsar.common.api.Commands;
import com.yahoo.pulsar.common.api.PulsarHandler;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandConnect;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;
import com.yahoo.pulsar.common.api.proto.PulsarApi.ServerError;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;

import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;

/**
 * Handles incoming discovery request from client and sends appropriate response back to client
 *
 */
public class ServerConnection extends PulsarHandler {

    private DiscoveryService service;
    private String authRole = null;
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
                authRole = service.getAuthenticationService()
                        .authenticate(new AuthenticationDataCommand(authData, remoteAddress, sslSession), authMethod);
                LOG.info("[{}] Client successfully authenticated with {} role {}", remoteAddress, authMethod, authRole);
            } catch (AuthenticationException e) {
                String msg = "Unable to authenticate";
                LOG.warn("[{}] {}: {}", remoteAddress, msg, e.getMessage());
                ctx.writeAndFlush(Commands.newError(-1, ServerError.AuthenticationError, msg));
                close();
                return;
            }
        }
        ctx.writeAndFlush(Commands.newConnected(connect));
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
            LoadReport availableBroker = service.getDiscoveryProvider().nextBroker();
            ctx.writeAndFlush(Commands.newLookupResponse(availableBroker.getPulsarServiceUrl(),
                    availableBroker.getPulsarServiceUrlTls(), false, Redirect, requestId));
        } catch (PulsarServerException e) {
            LOG.warn("[{}] Failed to get next active broker {}", remoteAddress, e.getMessage(), e);
            ctx.writeAndFlush(
                    Commands.newLookupResponse(ServerError.ServiceNotReady, e.getMessage(), requestId));
        }
    }

    private void sendPartitionMetadataResponse(CommandPartitionedTopicMetadata partitionMetadata) {
        final long requestId = partitionMetadata.getRequestId();
        DestinationName dn = DestinationName.get(partitionMetadata.getTopic());

        service.getDiscoveryProvider().getPartitionedTopicMetadata(service, dn, authRole).thenAccept(metadata -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Total number of partitions for topic {} is {}", authRole, dn, metadata.partitions);
            }
            ctx.writeAndFlush(Commands.newPartitionMetadataResponse(metadata.partitions, requestId));
        }).exceptionally(ex -> {
            LOG.warn("[{}] Failed to get partitioned metadata for topic {} {}", remoteAddress, dn, ex.getMessage(), ex);
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
