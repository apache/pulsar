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
package com.yahoo.pulsar.client.impl;

import static java.lang.String.format;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.common.api.Commands;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.partition.PartitionedTopicMetadata;

import io.netty.buffer.ByteBuf;

class BinaryProtoLookupService implements LookupService {

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
    public CompletableFuture<InetSocketAddress> getBroker(DestinationName destination) {
        return findBroker(serviceAddress, false, destination);
    }
    
    /**
     * calls broker binaryProto-lookup api to get metadata of partitioned-topic.
     * 
     */
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(DestinationName destination) {
        return getPartitionedTopicMetadata(serviceAddress, destination);
    }
    
    
    private CompletableFuture<InetSocketAddress> findBroker(InetSocketAddress socketAddress, boolean authoritative,
            DestinationName destination) {
        CompletableFuture<InetSocketAddress> addressFuture = new CompletableFuture<InetSocketAddress>();

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
                                .thenAccept(brokerAddress -> {
                                    addressFuture.complete(brokerAddress);
                                }).exceptionally((lookupException) -> {
                                    // lookup failed
                                    log.warn("[{}] lookup failed : {}", destination.toString(),
                                            lookupException.getMessage(), lookupException);
                                    addressFuture.completeExceptionally(lookupException);
                                    return null;
                                });
                    } else {
                        // (3) received correct broker to connect
                        addressFuture.complete(responseBrokerAddress);
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

    static class LookupDataResult {

        private String brokerUrl;
        private String brokerUrlTls;
        private int partitions;
        private boolean authoritative;
        private boolean redirect;

        public LookupDataResult(String brokerUrl, String brokerUrlTls, boolean redirect, boolean authoritative) {
            super();
            this.brokerUrl = brokerUrl;
            this.brokerUrlTls = brokerUrlTls;
            this.authoritative = authoritative;
            this.redirect = redirect;
        }
        
        public LookupDataResult(int partitions) {
            super();
            this.partitions = partitions;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(BinaryProtoLookupService.class);
}
