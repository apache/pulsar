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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.naming.DestinationName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.Counter;

public class LookupViaServiceUrl implements LookupProxyHandler {
    private final ProxyService service;
    private final ProxyConnection proxyConnection;
    private final boolean connectWithTLS;

    private SocketAddress clientAddress;

    private static final Counter lookupRequests = Counter
            .build("pulsar_proxy_lookup_requests", "Counter of topic lookup requests").create().register();

    private static final Counter partitionsMetadataRequests = Counter
            .build("pulsar_proxy_partitions_metadata_requests", "Counter of partitions metadata requests").create()
            .register();

    public LookupViaServiceUrl(ProxyService proxy, ProxyConnection proxyConnection) {
        this.service = proxy;
        this.proxyConnection = proxyConnection;
        this.clientAddress = proxyConnection.clientAddress();
        this.connectWithTLS = proxy.getConfiguration().isTlsEnabledWithBroker();
    }

    public void handleLookup(CommandLookupTopic lookup) {
        if (log.isDebugEnabled()) {
            log.debug("Received Lookup from {}", clientAddress);
        }

        lookupRequests.inc();
        long clientRequestId = lookup.getRequestId();
        String topic = lookup.getTopic();

        performLookup(clientRequestId, topic,
                this.connectWithTLS ? service.getConfiguration().getDiscoveryServiceURLTLS() : service.getConfiguration().getDiscoveryServiceURL(),
                false, 10);
    }

    private void performLookup(long clientRequestId, String topic, String brokerServiceUrl, boolean authoritative,
            int numberOfRetries) {
        if (numberOfRetries == 0) {
            proxyConnection.ctx().writeAndFlush(Commands.newLookupErrorResponse(ServerError.ServiceNotReady,
                    "Reached max number of redirections", clientRequestId));
            return;
        }

        URI brokerURI;
        try {
            brokerURI = new URI(brokerServiceUrl);
        } catch (URISyntaxException e) {
            proxyConnection.ctx().writeAndFlush(
                    Commands.newLookupErrorResponse(ServerError.MetadataError, e.getMessage(), clientRequestId));
            return;
        }

        InetSocketAddress addr = InetSocketAddress.createUnresolved(brokerURI.getHost(), brokerURI.getPort());
        if (log.isDebugEnabled()) {
            log.debug("Getting connections to '{}' for Looking up topic '{}' with clientReq Id '{}'", addr, topic, clientRequestId);
        }
        service.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
            // Connected to backend broker
            long requestId = service.newRequestId();
            clientCnx.newLookup(Commands.newLookup(topic, authoritative, proxyConnection.clientAuthRole, requestId), requestId).thenAccept(result -> {
                if (result.redirect) {
                    // Need to try the lookup again on a different broker
                    performLookup(clientRequestId, topic, result.brokerUrl, authoritative, numberOfRetries - 1);
                } else {
                    // We have the result immediately
                    String brokerUrl = connectWithTLS ? result.brokerUrlTls : result.brokerUrl;

                    // Reply the same address for both TLS non-TLS. The reason is that whether we use TLS between proxy
                    // and broker is independent of whether the client itself uses TLS, but we need to force the client
                    // to use the appropriate target broker (and port) when it will connect back.
                    proxyConnection.ctx().writeAndFlush(Commands.newLookupResponse(brokerUrl, brokerUrl, true,
                            LookupType.Connect, clientRequestId, true /* this is coming from proxy */));
                }
            }).exceptionally(ex -> {
                log.warn("[{}] Failed to lookup topic {}: {}", clientAddress, topic, ex.getMessage());
                proxyConnection.ctx().writeAndFlush(
                        Commands.newLookupErrorResponse(ServerError.ServiceNotReady, ex.getMessage(), clientRequestId));
                return null;
            });
        }).exceptionally(ex -> {
            // Failed to connect to backend broker
            proxyConnection.ctx().writeAndFlush(
                    Commands.newLookupErrorResponse(ServerError.ServiceNotReady, ex.getMessage(), clientRequestId));
            return null;
        });
    }

    public void handlePartitionMetadataResponse(CommandPartitionedTopicMetadata partitionMetadata) {
        partitionsMetadataRequests.inc();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received PartitionMetadataLookup", clientAddress);
        }

        final long clientRequestId = partitionMetadata.getRequestId();
        DestinationName dn = DestinationName.get(partitionMetadata.getTopic());
        String brokerServiceUrl = this.connectWithTLS ? service.getConfiguration().getDiscoveryServiceURLTLS()
                : service.getConfiguration().getDiscoveryServiceURL();
        URI brokerURI;
        try {
            brokerURI = new URI(brokerServiceUrl);
        } catch (URISyntaxException e) {
            proxyConnection.ctx().writeAndFlush(
                    Commands.newPartitionMetadataResponse(ServerError.MetadataError, e.getMessage(), clientRequestId));
            return;
        }
        InetSocketAddress addr = new InetSocketAddress(brokerURI.getHost(), brokerURI.getPort());

        if (log.isDebugEnabled()) {
            log.debug("Getting connections to '{}' for Looking up topic '{}' with clientReq Id '{}'", addr,
                    dn.getPartitionedTopicName(), clientRequestId);
        }

        service.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
            // Connected to backend broker
            long requestId = service.newRequestId();
            clientCnx.newLookup(Commands.newPartitionMetadataRequest(dn.toString(), requestId, proxyConnection.clientAuthRole), requestId).thenAccept(lookupDataResult -> {
                proxyConnection.ctx()
                .writeAndFlush(Commands.newPartitionMetadataResponse(lookupDataResult.partitions, clientRequestId));
            }).exceptionally((ex) -> {
                log.warn("[{}] failed to get Partitioned metadata : {}", dn.toString(),
                        ex.getCause().getMessage(), ex);
                proxyConnection.ctx().writeAndFlush(
                        Commands.newLookupErrorResponse(ServerError.ServiceNotReady, ex.getMessage(), clientRequestId));               
                return null;
            });
        }).exceptionally(ex -> {
            // Failed to connect to backend broker
            proxyConnection.ctx().writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.ServiceNotReady,
                    ex.getMessage(), clientRequestId));
            return null;
        });
    }

    private static final Logger log = LoggerFactory.getLogger(LookupViaServiceUrl.class);
}
