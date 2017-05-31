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
package org.apache.pulsar.client.impl;

import static java.lang.String.format;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

public class BinaryProtoLookupService implements LookupService {

    private final PulsarClientImpl client;
    protected final InetSocketAddress serviceAddress;
    private final boolean useTls;

    public BinaryProtoLookupService(PulsarClientImpl client, String serviceUrl, boolean useTls)
            throws PulsarClientException {
        this.client = client;
        this.useTls = useTls;
        URI uri;
        try {
            uri = new URI(serviceUrl);
            this.serviceAddress = new InetSocketAddress(uri.getHost(), uri.getPort());
        } catch (Exception e) {
            log.error("Invalid service-url {} provided {}", serviceUrl, e.getMessage(), e);
            throw new PulsarClientException.InvalidServiceURL(e);
        }
    }

    /**
     * Calls broker binaryProto-lookup api to find broker-service address which can serve a given topic.
     *
     * @param destination: topic-name
     * @return broker-socket-address that serves given topic
     */
    public CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> getBroker(DestinationName destination) {
        return findBroker(serviceAddress, false, destination);
    }

    /**
     * calls broker binaryProto-lookup api to get metadata of partitioned-topic.
     *
     */
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(DestinationName destination) {
        return getPartitionedTopicMetadata(serviceAddress, destination);
    }


    private CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> findBroker(InetSocketAddress socketAddress,
            boolean authoritative, DestinationName destination) {
        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> addressFuture = new CompletableFuture<>();

        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newLookup(destination.toString(), authoritative, requestId);
            clientCnx.newLookup(request, requestId).thenAccept(lookupDataResult -> {
                URI uri = null;
                try {
                    // (1) build response broker-address
                    if (useTls) {
                        uri = new URI(lookupDataResult.brokerUrlTls);
                    } else {
                        String serviceUrl = lookupDataResult.brokerUrl;
                        uri = new URI(serviceUrl);
                    }

                    InetSocketAddress responseBrokerAddress = new InetSocketAddress(uri.getHost(), uri.getPort());

                    // (2) redirect to given address if response is: redirect
                    if (lookupDataResult.redirect) {
                        findBroker(responseBrokerAddress, lookupDataResult.authoritative, destination)
                                .thenAccept(addressPair -> {
                                    addressFuture.complete(addressPair);
                                }).exceptionally((lookupException) -> {
                                    // lookup failed
                                    log.warn("[{}] lookup failed : {}", destination.toString(),
                                            lookupException.getMessage(), lookupException);
                                    addressFuture.completeExceptionally(lookupException);
                                    return null;
                                });
                    } else {
                        // (3) received correct broker to connect
                        if (lookupDataResult.proxyThroughServiceUrl) {
                            // Connect through proxy
                            addressFuture.complete(Pair.of(responseBrokerAddress, serviceAddress));
                        } else {
                            // Normal result with direct connection to broker
                            addressFuture.complete(Pair.of(responseBrokerAddress, responseBrokerAddress));
                        }
                    }

                } catch (Exception parseUrlException) {
                    // Failed to parse url
                    log.warn("[{}] invalid url {} : {}", destination.toString(), uri, parseUrlException.getMessage(),
                            parseUrlException);
                    addressFuture.completeExceptionally(parseUrlException);
                }
            }).exceptionally((sendException) -> {
                // lookup failed
                log.warn("[{}] failed to send lookup request : {}", destination.toString(), sendException.getMessage(),
                        sendException);
                addressFuture.completeExceptionally(sendException);
                return null;
            });
        }).exceptionally(connectionException -> {
            addressFuture.completeExceptionally(connectionException);
            return null;
        });
        return addressFuture;
    }


    private CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(InetSocketAddress socketAddress,
            DestinationName destination) {

        CompletableFuture<PartitionedTopicMetadata> partitionFuture = new CompletableFuture<PartitionedTopicMetadata>();

        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newPartitionMetadataRequest(destination.toString(), requestId);
            clientCnx.newLookup(request, requestId).thenAccept(lookupDataResult -> {
                try {
                    partitionFuture.complete(new PartitionedTopicMetadata(lookupDataResult.partitions));
                } catch (Exception e) {
                    partitionFuture.completeExceptionally(new PulsarClientException.LookupException(
                            format("Failed to parse partition-response redirect=%s , partitions with %s",
                                    lookupDataResult.redirect, lookupDataResult.partitions, e.getMessage())));
                }
            }).exceptionally((e) -> {
                log.warn("[{}] failed to get Partitioned metadata : {}", destination.toString(),
                        e.getCause().getMessage(), e);
                partitionFuture.completeExceptionally(e);
                return null;
            });
        }).exceptionally(connectionException -> {
            partitionFuture.completeExceptionally(connectionException);
            return null;
        });

        return partitionFuture;
    }

    public String getServiceUrl() {
    	return serviceAddress.toString();
    }

    @Override
    public void close() throws Exception {
        // no-op
    }

    public static class LookupDataResult {

        public final String brokerUrl;
        public final String brokerUrlTls;
        public final int partitions;
        public final boolean authoritative;
        public final boolean proxyThroughServiceUrl;
        public final boolean redirect;

        public LookupDataResult(CommandLookupTopicResponse result) {
            this.brokerUrl = result.getBrokerServiceUrl();
            this.brokerUrlTls = result.getBrokerServiceUrlTls();
            this.authoritative = result.getAuthoritative();
            this.redirect = result.getResponse() == LookupType.Redirect;
            this.proxyThroughServiceUrl = result.getProxyThroughServiceUrl();
            this.partitions = -1;
        }

        public LookupDataResult(int partitions) {
            super();
            this.partitions = partitions;
            this.brokerUrl = null;
            this.brokerUrlTls = null;
            this.authoritative = false;
            this.proxyThroughServiceUrl = false;
            this.redirect = false;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(BinaryProtoLookupService.class);
}
