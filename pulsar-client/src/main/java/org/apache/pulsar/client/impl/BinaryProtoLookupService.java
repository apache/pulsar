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

import com.google.common.collect.Lists;

import io.netty.buffer.ByteBuf;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandBatchLookupTopicResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandBatchLookupTopicResponse.ResponseType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryProtoLookupService implements LookupService {

    private final PulsarClientImpl client;
    private final ServiceNameResolver serviceNameResolver;
    private final boolean useTls;
    private final ExecutorService executor;

    public BinaryProtoLookupService(PulsarClientImpl client, String serviceUrl, boolean useTls, ExecutorService executor)
            throws PulsarClientException {
        this.client = client;
        this.useTls = useTls;
        this.executor = executor;
        this.serviceNameResolver = new PulsarServiceNameResolver();
        updateServiceUrl(serviceUrl);
    }

    @Override
    public void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        serviceNameResolver.updateServiceUrl(serviceUrl);
    }

    /**
     * Calls broker binaryProto-lookup api to find broker-service address which can serve a given topic.
     *
     * @param topicName
     *            topic-name
     * @return broker-socket-address that serves given topic
     */
    public CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> getBroker(TopicName topicName) {
        return findBroker(serviceNameResolver.resolveHost(), false, topicName);
    }

    public CompletableFuture<BatchLookupResult> getBrokers(List<TopicName> topicNames) {
        return findBrokers(serviceNameResolver.resolveHost(), false, topicNames, true);
    }

    /**
     * calls broker binaryProto-lookup api to get metadata of partitioned-topic.
     *
     */
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(TopicName topicName) {
        return getPartitionedTopicMetadata(serviceNameResolver.resolveHost(), topicName);
    }

    private CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> findBroker(InetSocketAddress socketAddress,
            boolean authoritative, TopicName topicName) {
        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> addressFuture = new CompletableFuture<>();

        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newLookup(topicName.toString(), authoritative, requestId);
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

                    InetSocketAddress responseBrokerAddress = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());

                    // (2) redirect to given address if response is: redirect
                    if (lookupDataResult.redirect) {
                        findBroker(responseBrokerAddress, lookupDataResult.authoritative, topicName)
                                .thenAccept(addressPair -> {
                                    addressFuture.complete(addressPair);
                                }).exceptionally((lookupException) -> {
                                    // lookup failed
                                    log.warn("[{}] lookup failed : {}", topicName.toString(),
                                            lookupException.getMessage(), lookupException);
                                    addressFuture.completeExceptionally(lookupException);
                                    return null;
                                });
                    } else {
                        // (3) received correct broker to connect
                        if (lookupDataResult.proxyThroughServiceUrl) {
                            // Connect through proxy
                            addressFuture.complete(Pair.of(responseBrokerAddress, socketAddress));
                        } else {
                            // Normal result with direct connection to broker
                            addressFuture.complete(Pair.of(responseBrokerAddress, responseBrokerAddress));
                        }
                    }

                } catch (Exception parseUrlException) {
                    // Failed to parse url
                    log.warn("[{}] invalid url {} : {}", topicName.toString(), uri, parseUrlException.getMessage(),
                            parseUrlException);
                    addressFuture.completeExceptionally(parseUrlException);
                }
            }).exceptionally((sendException) -> {
                // lookup failed
                log.warn("[{}] failed to send lookup request : {}", topicName.toString(), sendException.getMessage());
                if (log.isDebugEnabled()) {
                    log.warn("[{}] Lookup response exception: {}", topicName.toString(), sendException);
                }

                addressFuture.completeExceptionally(sendException);
                return null;
            });
        }).exceptionally(connectionException -> {
            addressFuture.completeExceptionally(connectionException);
            return null;
        });
        return addressFuture;
    }

    private CompletableFuture<BatchLookupResult> findBrokers(InetSocketAddress socketAddress,
                                                                boolean authoritative, List<TopicName> topicNames,
                                                                boolean doRedirectLookup) {
        CompletableFuture<BatchLookupResult> batchLookupFuture = new CompletableFuture<>();
        Map<Pair<InetSocketAddress, InetSocketAddress>, List<TopicName>> succeedLookup = new HashMap<>();
        List<CompletableFuture<BatchLookupResult>> redirectedLookupFutures = new ArrayList<>();
        List<TopicName> failedTopicNames = new ArrayList<>();
        List<TopicName> unauthorizedTopicNames = new ArrayList<>();

        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf batchLookupRequest = Commands.newBatchLookup(
                    topicNames.stream().map(topicName -> topicName.toString()). collect(Collectors.toList()), authoritative, requestId);
            clientCnx.newBatchLookup(batchLookupRequest, requestId).thenAccept(batchLookupDataResult -> {
                // For succeeded lookup add them to final result.
                batchLookupDataResult.lookupResults.entrySet().forEach(batchLookupResult -> {
                    URI uri = null;
                    try {
                        if (useTls) {
                            uri = new URI(batchLookupResult.getKey().getLeft().getLeft());
                        } else {
                            uri = new URI(batchLookupResult.getKey().getLeft().getRight());
                        }

                        InetSocketAddress responseBrokerAddress = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());

                            if (batchLookupResult.getKey().getRight()) {
                                // Connect through proxy
                                succeedLookup.put(Pair.of(responseBrokerAddress, socketAddress),
                                        batchLookupResult.getValue().stream().map(topic -> TopicName.get(topic))
                                                .collect(Collectors.toList()));
                            } else {
                                // Normal result with direct connection to broker
                                succeedLookup.put(Pair.of(responseBrokerAddress, responseBrokerAddress),
                                        batchLookupResult.getValue().stream().map(topic -> TopicName.get(topic))
                                                .collect(Collectors.toList()));
                            }
                        } catch (Exception parseUrlException) {
                        // Failed to parse url, caller have to retry.
                        log.warn("Get invalid url when trying to construct lookup result {} : {}",
                                uri, parseUrlException.getMessage(), parseUrlException);
                        failedTopicNames.addAll(batchLookupResult.getValue().stream().map(topic -> TopicName.get(topic))
                                .collect(Collectors.toList()));
                    }
                });
                if (doRedirectLookup) {
                    batchLookupDataResult.redirectTopics.entrySet().forEach(redirectTopics -> {
                        URI uri = null;
                        try {
                            if (useTls) {
                                uri = new URI(redirectTopics.getKey().getLeft());
                            } else {
                                uri = new URI(redirectTopics.getKey().getRight());
                            }
                            InetSocketAddress responseBrokerAddress = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());
                            redirectedLookupFutures.add(findBrokers(responseBrokerAddress, true,
                                    redirectTopics.getValue().stream().map(topic -> TopicName.get(topic)).
                                            collect(Collectors.toList()), false));

                        } catch (Exception parseUrlException) {
                            // Failed to parse url, caller have to retry.
                            log.warn("Get invalid url when trying to construct lookup result {} : {}",
                                    uri, parseUrlException.getMessage(), parseUrlException);
                            failedTopicNames.addAll(redirectTopics.getValue().stream().map(topic -> TopicName.get(topic))
                                    .collect(Collectors.toList()));
                        }
                    });
                } else {
                    // Already try lookup for topics with redirect response, and still getting
                    // redirect response. Let caller to do retry as this method can not do recursive
                    // lookup till succeed.
                    failedTopicNames.addAll(batchLookupDataResult.redirectTopics.values().stream()
                            .flatMap(redirectTopics -> redirectTopics.stream().map(topic -> TopicName.get(topic)))
                            .collect(Collectors.toList()));
                }

                // Handle retried lookup response for redirect.
                FutureUtil.waitForAll(redirectedLookupFutures).thenRun(() -> {
                    redirectedLookupFutures.forEach(redirectedLookupFuture -> {
                        try {
                            BatchLookupResult redirectLookupResult = redirectedLookupFuture.get();
                            succeedLookup.putAll(redirectLookupResult.getLookupResult());
                            failedTopicNames.addAll(redirectLookupResult.getFailedTopics());
                            unauthorizedTopicNames.addAll(redirectLookupResult.getUnauthorizedTopics());
                        } catch (Exception e) {
                            log.warn("Redirected lookup request failed ", e);
                        }
                    });
                });

                unauthorizedTopicNames.addAll(batchLookupDataResult.unauthorizedTopics.stream().
                        map(topic -> TopicName.get(topic)).collect(Collectors.toList()));

                batchLookupFuture.complete(new BatchLookupResult(succeedLookup, failedTopicNames, unauthorizedTopicNames));

            }).exceptionally((sendException) -> {
                // lookup failed
                log.warn("Failed to send batch lookup request : {}", sendException.getMessage(),
                        sendException instanceof ClosedChannelException ? null : sendException);
                batchLookupFuture.completeExceptionally(sendException);
                return null;
            });
        }).exceptionally(connectionException -> {
            batchLookupFuture.completeExceptionally(connectionException);
            return null;
        });
        return batchLookupFuture;
    }

    private CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(InetSocketAddress socketAddress,
            TopicName topicName) {

        CompletableFuture<PartitionedTopicMetadata> partitionFuture = new CompletableFuture<PartitionedTopicMetadata>();

        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newPartitionMetadataRequest(topicName.toString(), requestId);
            clientCnx.newLookup(request, requestId).thenAccept(lookupDataResult -> {
                try {
                    partitionFuture.complete(new PartitionedTopicMetadata(lookupDataResult.partitions));
                } catch (Exception e) {
                    partitionFuture.completeExceptionally(new PulsarClientException.LookupException(
                            format("Failed to parse partition-response redirect=%s , partitions with %s",
                                    lookupDataResult.redirect, lookupDataResult.partitions, e.getMessage())));
                }
            }).exceptionally((e) -> {
                log.warn("[{}] failed to get Partitioned metadata : {}", topicName.toString(),
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

    @Override
    public CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName) {
        return getSchema(topicName, null);
    }


    @Override
    public CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName, byte[] version) {
        return client.getCnxPool().getConnection(serviceNameResolver.resolveHost()).thenCompose(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newGetSchema(requestId, topicName.toString(),
                    Optional.ofNullable(BytesSchemaVersion.of(version)));
            return clientCnx.sendGetSchema(request, requestId);
        });
    }

    public String getServiceUrl() {
        return serviceNameResolver.getServiceUrl();
    }

    @Override
    public CompletableFuture<List<String>> getTopicsUnderNamespace(NamespaceName namespace, Mode mode) {
        CompletableFuture<List<String>> topicsFuture = new CompletableFuture<List<String>>();

        AtomicLong opTimeoutMs = new AtomicLong(client.getConfiguration().getOperationTimeoutMs());
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMandatoryStop(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                .setMax(0, TimeUnit.MILLISECONDS)
                .useUserConfiguredIntervals(client.getConfiguration().getDefaultBackoffIntervalNanos(),
                                            client.getConfiguration().getMaxBackoffIntervalNanos())
                .create();
        getTopicsUnderNamespace(serviceNameResolver.resolveHost(), namespace, backoff, opTimeoutMs, topicsFuture, mode);
        return topicsFuture;
    }

    private void getTopicsUnderNamespace(InetSocketAddress socketAddress,
                                         NamespaceName namespace,
                                         Backoff backoff,
                                         AtomicLong remainingTime,
                                         CompletableFuture<List<String>> topicsFuture,
                                         Mode mode) {
        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newGetTopicsOfNamespaceRequest(
                namespace.toString(), requestId, mode);

            clientCnx.newGetTopicsOfNamespace(request, requestId).thenAccept(topicsList -> {
                if (log.isDebugEnabled()) {
                    log.debug("[namespace: {}] Success get topics list in request: {}", namespace.toString(), requestId);
                }

                // do not keep partition part of topic name
                List<String> result = Lists.newArrayList();
                topicsList.forEach(topic -> {
                    String filtered = TopicName.get(topic).getPartitionedTopicName();
                    if (!result.contains(filtered)) {
                        result.add(filtered);
                    }
                });

                topicsFuture.complete(result);
            }).exceptionally((e) -> {
                topicsFuture.completeExceptionally(e);
                return null;
            });
        }).exceptionally((e) -> {
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            if (nextDelay <= 0) {
                topicsFuture.completeExceptionally(new PulsarClientException
                    .TimeoutException("Could not getTopicsUnderNamespace within configured timeout."));
                return null;
            }

            ((ScheduledExecutorService) executor).schedule(() -> {
                log.warn("[namespace: {}] Could not get connection while getTopicsUnderNamespace -- Will try again in {} ms",
                    namespace, nextDelay);
                remainingTime.addAndGet(-nextDelay);
                getTopicsUnderNamespace(socketAddress, namespace, backoff, remainingTime, topicsFuture, mode);
            }, nextDelay, TimeUnit.MILLISECONDS);
            return null;
        });
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

    public static class BatchLookupDataResult {
        public final Map<Pair<Pair<String, String>, Boolean>, List<String>> lookupResults = new HashMap<>();
        public final Map<Pair<String, String>, List<String>> redirectTopics = new HashMap<>();
        public final List<String> unauthorizedTopics;
        public final ResponseType response;
        public final boolean authoritative;

        public BatchLookupDataResult(CommandBatchLookupTopicResponse result) {
            this.unauthorizedTopics = result.getUnauthorizedTopicsList();
            this.authoritative = result.getAuthoritative();
            result.getLookupResponsesList().forEach(lookupResult -> {
                if (LookupType.Connect.equals(lookupResult.getResponse())) {
                    lookupResults.put(Pair.of(Pair.of(lookupResult.getBrokerServiceUrl(),
                            lookupResult.getBrokerServiceUrlTls()), lookupResult.getProxyThroughServiceUrl()),
                            lookupResult.getTopicsList());
                } else if (LookupType.Redirect.equals(lookupResult.getResponse())) {
                    redirectTopics.put(Pair.of(lookupResult.getBrokerServiceUrl(), lookupResult.getBrokerServiceUrlTls()),
                            lookupResult.getTopicsList());
                }
            });
            response = (0 == redirectTopics.size()) ? ResponseType.Success : ResponseType.PartialSuccess;
        }
    }

    @AllArgsConstructor
    @Getter
    private static class BatchLookupResult {
        private final Map<Pair<InetSocketAddress, InetSocketAddress>, List<TopicName>> lookupResult;
        private List<TopicName> failedTopics;
        private List<TopicName> unauthorizedTopics;
    }

    private static final Logger log = LoggerFactory.getLogger(BinaryProtoLookupService.class);
}
