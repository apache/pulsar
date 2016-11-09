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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.common.api.Commands;
import com.yahoo.pulsar.common.api.PulsarHandler;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandConnect;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;
import com.yahoo.pulsar.common.api.proto.PulsarApi.ServerError;
import com.yahoo.pulsar.common.policies.data.loadbalancer.LoadReport;

/**
 * Handles incoming discovery request from client and sends appropriate response back to client
 *
 */
public class ServerConnection extends PulsarHandler {

    private BrokerDiscoveryProvider discoveryProvider;
    private State state;

    enum State {
        Start, Connected
    }

    public ServerConnection(BrokerDiscoveryProvider discoveryhandler) {
        super(0, TimeUnit.SECONDS); // discovery-service doesn't need to run keepAlive task
        this.discoveryProvider = discoveryhandler;
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
        sendLookupResponse(partitionMetadata.getRequestId());
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

    private void sendLookupResponse(long requestId) {
        try {
            LoadReport availableBroker = discoveryProvider.nextBroker();
            ctx.writeAndFlush(Commands.newLookupResponse(availableBroker.getPulsarServiceUrl(),
                    availableBroker.getPulsarServieUrlTls(), false, Redirect, requestId));
        } catch (PulsarServerException e) {
            LOG.warn("[{}] Failed to get next active broker {}", remoteAddress, e.getMessage(), e);
            ctx.writeAndFlush(
                    Commands.newLookupResponse(ServerError.ServiceNotReady, e.getMessage(), requestId));
        }
    }

    @Override
    protected boolean isHandshakeCompleted() {
        return state == State.Connected;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ServerConnection.class);

}
