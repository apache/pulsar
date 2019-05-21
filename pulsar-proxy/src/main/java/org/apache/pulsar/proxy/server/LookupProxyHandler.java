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

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.impl.BinaryProtoLookupService.BatchLookupResult;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetSchema;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandBatchLookupTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.ServerError;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.prometheus.client.Counter;

import static org.apache.pulsar.common.api.proto.PulsarApi.CommandBatchLookupTopicResponse;
import static org.apache.pulsar.common.api.proto.PulsarApi.CommandBatchLookupTopicResponse.ResponseType;
import static org.apache.pulsar.client.impl.BinaryProtoLookupService.BatchLookupDataResult;

public class LookupProxyHandler {
    private final String throttlingErrorMessage = "Too many concurrent lookup and partitionsMetadata requests";
    private static final String BATCH_LOOKUP_THROTTLING_ERROR_MESSAGE = "Too many concurrent batch lookup requests.";
    private static final int LOOKUP_RETRY_NUMBER = 10;
    private final ProxyService service;
    private final ProxyConnection proxyConnection;
    private final boolean connectWithTLS;

    private SocketAddress clientAddress;
    private String brokerServiceURL;

    private static final Counter lookupRequests = Counter
            .build("pulsar_proxy_lookup_requests", "Counter of topic lookup requests").create().register();

    private static final Counter batchLookupRequests = Counter
            .build("pulsar_proxy_batch_lookup_requests", "Counter of batch topic lookup requests")
            .create()
            .register();

    private static final Counter partitionsMetadataRequests = Counter
            .build("pulsar_proxy_partitions_metadata_requests", "Counter of partitions metadata requests").create()
            .register();

    private static final Counter getTopicsOfNamespaceRequestss = Counter
            .build("pulsar_proxy_get_topics_of_namespace_requests", "Counter of getTopicsOfNamespace requests")
            .create()
            .register();

    private static final Counter getSchemaRequests = Counter
            .build("pulsar_proxy_get_schema_requests", "Counter of schema requests")
            .create()
            .register();

    static final Counter rejectedLookupRequests = Counter.build("pulsar_proxy_rejected_lookup_requests",
            "Counter of topic lookup requests rejected due to throttling").create().register();

    static final Counter rejectedBatchLookupRequests = Counter.build("pulsar_proxy_rejected_Batch_lookup_requests",
            "Counter of batch topic lookup requests rejected due to throttling").create().register();

    static final Counter rejectedPartitionsMetadataRequests = Counter
            .build("pulsar_proxy_rejected_partitions_metadata_requests",
                    "Counter of partitions metadata requests rejected due to throttling")
            .create().register();

    static final Counter rejectedGetTopicsOfNamespaceRequests = Counter
            .build("pulsar_proxy_rejected_get_topics_of_namespace_requests",
                    "Counter of getTopicsOfNamespace requests rejected due to throttling")
            .create().register();

    public LookupProxyHandler(ProxyService proxy, ProxyConnection proxyConnection) {
        this.service = proxy;
        this.proxyConnection = proxyConnection;
        this.clientAddress = proxyConnection.clientAddress();
        this.connectWithTLS = proxy.getConfiguration().isTlsEnabledWithBroker();
        this.brokerServiceURL = this.connectWithTLS ? proxy.getConfiguration().getBrokerServiceURLTLS()
                : proxy.getConfiguration().getBrokerServiceURL();
    }

    public void handleLookup(CommandLookupTopic lookup) {
        if (log.isDebugEnabled()) {
            log.debug("Received Lookup from {}", clientAddress);
        }
        long clientRequestId = lookup.getRequestId();
        if (this.service.getLookupRequestSemaphore().tryAcquire()) {
            lookupRequests.inc();
            String topic = lookup.getTopic();
            String serviceUrl;
            if (isBlank(brokerServiceURL)) {
                ServiceLookupData availableBroker = null;
                try {
                    availableBroker = service.getDiscoveryProvider().nextBroker();
                } catch (Exception e) {
                    log.warn("[{}] Failed to get next active broker {}", clientAddress, e.getMessage(), e);
                    proxyConnection.ctx().writeAndFlush(Commands.newLookupErrorResponse(ServerError.ServiceNotReady,
                            e.getMessage(), clientRequestId));
                    return;
                }
                serviceUrl = this.connectWithTLS ? availableBroker.getPulsarServiceUrlTls()
                        : availableBroker.getPulsarServiceUrl();
            } else {
                serviceUrl = this.connectWithTLS ? service.getConfiguration().getBrokerServiceURLTLS()
                        : service.getConfiguration().getBrokerServiceURL();
            }
            performLookup(clientRequestId, topic, serviceUrl, false, 10);
            this.service.getLookupRequestSemaphore().release();
        } else {
            rejectedLookupRequests.inc();
            if (log.isDebugEnabled()) {
                log.debug("Lookup Request ID {} from {} rejected - {}.", clientRequestId, clientAddress,
                        throttlingErrorMessage);
            }
            proxyConnection.ctx().writeAndFlush(Commands.newLookupErrorResponse(ServerError.ServiceNotReady,
                    throttlingErrorMessage, clientRequestId));
        }

    }

    public void handleBatchLookup(CommandBatchLookupTopic batchLookup) {
        if (log.isDebugEnabled()) {
            log.debug("Received Batch Lookup request from {}", clientAddress);
        }
        long requestId = batchLookup.getRequestId();
        if (this.service.getBatchLookupRequestsSemehpore().tryAcquire()) {
            batchLookupRequests.inc();
            List<String> topicNames = batchLookup.getTopicList();
            String serviceUrl;
            if (isBlank(brokerServiceURL)) {
                ServiceLookupData availableBroker = null;
                try {
                    availableBroker = service.getDiscoveryProvider().nextBroker();
                } catch (Exception e) {
                    log.warn("[{}] Failed to get next active broker {}", clientAddress, e.getMessage(), e);
                    proxyConnection.ctx().writeAndFlush(Commands.newBatchLookupErrorResponse(ServerError.ServiceNotReady,
                            e.getMessage(), requestId));
                    return;
                }
                serviceUrl = this.connectWithTLS ? availableBroker.getPulsarServiceUrlTls()
                        : availableBroker.getPulsarServiceUrl();
            } else {
                serviceUrl = this.connectWithTLS ? service.getConfiguration().getBrokerServiceURLTLS()
                        : service.getConfiguration().getBrokerServiceURL();
            }
            Map<Pair<String, String>, List<String>> succeedLookup = new HashMap<>();
            Map<Pair<String, String>, List<String>> redirectedLookup = new HashMap<>();
            List<String> failedTopicNames = new ArrayList<>();
            List<String> unauthorizedTopicNames = new ArrayList<>();
            long batchLookupRequestId = proxyConnection.newRequestId();
            performBatchLookup(requestId, topicNames, serviceUrl, false, 10, failedTopicNames,
                    succeedLookup, redirectedLookup, unauthorizedTopicNames, batchLookupRequestId);
            this.service.getBatchLookupRequestsSemehpore().release();
        } else {
            rejectedBatchLookupRequests.inc();
            if (log.isDebugEnabled()) {
                log.debug("Batch Lookup Request:{} from {} rejected - {}.", requestId, clientAddress,
                        BATCH_LOOKUP_THROTTLING_ERROR_MESSAGE);
            }
            proxyConnection.ctx().writeAndFlush(Commands.newBatchLookupErrorResponse(ServerError.ServiceNotReady,
                    BATCH_LOOKUP_THROTTLING_ERROR_MESSAGE, requestId));
        }
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
            log.debug("Getting connections to '{}' for Looking up topic '{}' with clientReq Id '{}'", addr, topic,
                    clientRequestId);
        }
        proxyConnection.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
            // Connected to backend broker
            long requestId = proxyConnection.newRequestId();
            ByteBuf command;
            command = Commands.newLookup(topic, authoritative, requestId);
            clientCnx.newLookup(command, requestId).thenAccept(result -> {
                String brokerUrl = connectWithTLS ? result.brokerUrlTls : result.brokerUrl;
                if (result.redirect) {
                    // Need to try the lookup again on a different broker
                    performLookup(clientRequestId, topic, brokerUrl, result.authoritative, numberOfRetries - 1);
                } else {
                    // Reply the same address for both TLS non-TLS. The reason
                    // is that whether we use TLS
                    // and broker is independent of whether the client itself
                    // uses TLS, but we need to force the
                    // client
                    // to use the appropriate target broker (and port) when it
                    // will connect back.
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

    private CompletableFuture<BatchLookupDataResult> performBatchLookupAsync(long clientRequestId, List<String> topicNames,
                                                    String brokerServiceUrl, boolean authoritative, int numberOfRetries,
                                                    final List<String> failedTopicNames, final Map<Pair<String, String>,
                                                    List<String>> succeedLookup, Map<Pair<String, String>, List<String>>
                                                    redirectedLookup, final List<String> unauthorizedTopicNames,
                                                    long batchLookupRequestId) {
        CompletableFuture<BatchLookupDataResult> future = new CompletableFuture<>();
        if (numberOfRetries == 0) {
            failedTopicNames.addAll(topicNames);
            future.complete(new BatchLookupDataResult(buildCommandBatchLookupRepose(failedTopicNames, authoritative,
                                                        succeedLookup, redirectedLookup, unauthorizedTopicNames, clientRequestId)));
            return future;
        }

        URI brokerURI;
        try {
            brokerURI = new URI(brokerServiceUrl);
        } catch (URISyntaxException e) {
            failedTopicNames.addAll(topicNames);
            future.complete(new BatchLookupDataResult(buildCommandBatchLookupRepose(failedTopicNames, authoritative,
                    succeedLookup, redirectedLookup, unauthorizedTopicNames, clientRequestId)));
            return future;
        }

        InetSocketAddress addr = InetSocketAddress.createUnresolved(brokerURI.getHost(), brokerURI.getPort());
        if (log.isDebugEnabled()) {
            log.debug("Getting connections to '{}' for batch topic look up with clientReq Id '{}', retry remain {} ",
                                                                                    addr, clientRequestId, numberOfRetries);
        }
        proxyConnection.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
            // Connected to backend broker
            ByteBuf request = Commands.newBatchLookup(topicNames, authoritative, batchLookupRequestId);
            clientCnx.newBatchLookup(request, batchLookupRequestId).thenAccept(batchLookupDataResult -> {
                batchLookupDataResult.lookupResults.entrySet().forEach(entry -> {
                    String brokerUrl = connectWithTLS ? entry.getKey().getLeft().getRight() :
                            entry.getKey().getLeft().getLeft();
                    succeedLookup.put(Pair.of(brokerUrl, brokerUrl), entry.getValue());
                });

                List<CompletableFuture<BatchLookupDataResult>> redirectedLookupFutures = new ArrayList<>();
                if (batchLookupDataResult.redirectTopics.size() != 0) {
                    // Need to try the lookup again on a different broker
                    batchLookupDataResult.redirectTopics.entrySet().forEach(entry -> {
                        String brokerUrl = connectWithTLS ? entry.getKey().getLeft() : entry.getKey().getLeft();
                        redirectedLookupFutures.add(performBatchLookupAsync(clientRequestId, entry.getValue(), brokerUrl, authoritative,
                                numberOfRetries - 1, failedTopicNames, succeedLookup, redirectedLookup,
                                unauthorizedTopicNames, batchLookupRequestId));
                    });
                }

                FutureUtil.waitForAll(redirectedLookupFutures).thenRun(() -> {
                    redirectedLookupFutures.forEach(redirectedLookupFuture -> {
                        try {
                            BatchLookupDataResult redirectLookupResult = redirectedLookupFuture.get();
                            redirectLookupResult.lookupResults.entrySet().forEach(entry -> {
                                String brokerUrl = connectWithTLS ? entry.getKey().getLeft().getRight() :
                                        entry.getKey().getLeft().getLeft();
                                succeedLookup.put(Pair.of(brokerUrl, brokerUrl), entry.getValue());
                            });
                            // These topics have reached max retry, mark as failed. As we have 10 retry and
                            redirectLookupResult.redirectTopics.entrySet().forEach(entry -> {
                                redirectedLookup.put(Pair.of(entry.getKey().getLeft(), entry.getKey().getLeft()), entry.getValue());
                            });
                            unauthorizedTopicNames.addAll(redirectLookupResult.unauthorizedTopics);
                            failedTopicNames.addAll(redirectLookupResult.failedTopics);
                        } catch (Exception e) {
                            log.warn("Redirected lookup request failed ", e);
                        }
                    });
                    future.complete(new BatchLookupDataResult(buildCommandBatchLookupRepose(failedTopicNames, authoritative,
                            succeedLookup, redirectedLookup, unauthorizedTopicNames, clientRequestId)));
                }).exceptionally(ex -> {
                    log.warn("[{}] Failed to lookup topic {}: {}", clientAddress, ex.getMessage());
                    failedTopicNames.addAll(batchLookupDataResult.redirectTopics.entrySet().stream().
                                        flatMap(entry -> entry.getValue().stream()).collect(Collectors.toList()));
                    future.complete(new BatchLookupDataResult(buildCommandBatchLookupRepose(failedTopicNames, authoritative,
                            succeedLookup, redirectedLookup, unauthorizedTopicNames, clientRequestId)));
                    return null;
                });

            }).exceptionally(ex -> {
                log.warn("[{}] Failed to lookup topic {}: {}", clientAddress, ex.getMessage());
                failedTopicNames.addAll(topicNames);
                future.complete(new BatchLookupDataResult(buildCommandBatchLookupRepose(failedTopicNames, authoritative,
                        succeedLookup, redirectedLookup, unauthorizedTopicNames, clientRequestId)));
                return null;
            });
        }).exceptionally(ex -> {
            // Failed to connect to backend broker
            failedTopicNames.addAll(topicNames);
            future.complete(new BatchLookupDataResult(buildCommandBatchLookupRepose(failedTopicNames, authoritative,
                    succeedLookup, redirectedLookup, unauthorizedTopicNames, clientRequestId)));
            return null;
        });
        return future;
    }

    private void performBatchLookup(long clientRequestId, List<String> topicNames, String brokerServiceUrl,
                                    boolean authoritative, int numberOfRetries, final List<String> failedTopicNames,
                                    final Map<Pair<String, String>, List<String>> succeedLookup,
                                    final Map<Pair<String, String>, List<String>> redirectedLookupFutures,
                                    List<String> unauthorizedTopicNames, long batchLookupRequestId) {
        try {
            BatchLookupDataResult batchLookupDataResult = performBatchLookupAsync(clientRequestId, topicNames, brokerServiceUrl,
                                authoritative, numberOfRetries, failedTopicNames, succeedLookup, redirectedLookupFutures,
                                unauthorizedTopicNames, batchLookupRequestId).get();
            Map<Pair<String, String>, List<String>> succeedLookupResult = new HashMap<>();
            batchLookupDataResult.lookupResults.entrySet().
                    forEach(entry -> succeedLookupResult.put(entry.getKey().getLeft(), entry.getValue()));
            proxyConnection.ctx().writeAndFlush(buildBatchLookupRepose(batchLookupDataResult.failedTopics, authoritative,
                    succeedLookupResult, batchLookupDataResult.redirectTopics, batchLookupDataResult.unauthorizedTopics,
                    clientRequestId));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private ByteBuf buildBatchLookupRepose(final List<String> failedTopicNames, boolean authoritative,
                                           final Map<Pair<String, String>, List<String>> succeedLookup,
                                           final Map<Pair<String, String>, List<String>> redirectedLookup,
                                           List<String> unauthorizedTopicNames, long requestId) {
        List<PulsarApi.CommandLookupTopicResponse> lookupTopicResponses = new ArrayList<>();
        populateLookupTopicResponses(failedTopicNames, authoritative, succeedLookup, redirectedLookup, lookupTopicResponses, requestId);
        return Commands.newBatchLookupResponse(lookupTopicResponses, (failedTopicNames.size() | redirectedLookup.size()) == 0 ?
                                               ResponseType.Success : ResponseType.PartialSuccess,
                                               unauthorizedTopicNames, requestId, authoritative);
    }

    private CommandBatchLookupTopicResponse buildCommandBatchLookupRepose(final List<String> failedTopicNames, boolean authoritative,
                                                                          final Map<Pair<String, String>, List<String>> succeedLookup,
                                                                          final Map<Pair<String, String>, List<String>> redirectedLookup,
                                                                          List<String> unauthorizedTopicNames, long requestId) {
        List<PulsarApi.CommandLookupTopicResponse> lookupTopicResponses = new ArrayList<>();
        populateLookupTopicResponses(failedTopicNames, authoritative, succeedLookup, redirectedLookup, lookupTopicResponses, requestId);
        return Commands.newCommandBatchLookupResponse(lookupTopicResponses,  (failedTopicNames.size() | redirectedLookup.size()) == 0 ?
                                                      ResponseType.Success: ResponseType.PartialSuccess,
                                                      unauthorizedTopicNames, requestId, authoritative);
    }

    private void populateLookupTopicResponses(final List<String> failedTopicNames, boolean authoritative,
                                              final Map<Pair<String, String>, List<String>> succeedLookup,
                                              final Map<Pair<String, String>, List<String>> redirectedLookup,
                                              List<PulsarApi.CommandLookupTopicResponse> lookupTopicResponses,
                                              long requestId) {
        succeedLookup.entrySet().forEach(entry -> {
            lookupTopicResponses.add(Commands.newCommandLookupResponse(entry.getKey().getLeft(),
                    entry.getKey().getRight(),
                    authoritative, LookupType.Connect, requestId,
                    this.connectWithTLS, entry.getValue()));
        });
        redirectedLookup.entrySet().forEach(entry -> {
            lookupTopicResponses.add(Commands.newCommandLookupResponse(entry.getKey().getLeft(),
                    entry.getKey().getRight(),
                    authoritative, LookupType.Redirect, requestId,
                    this.connectWithTLS, entry.getValue()));
        });
        lookupTopicResponses.add(Commands.newCommandLookupErrorResponse(null, null, requestId, failedTopicNames));
    }

    public void handlePartitionMetadataResponse(CommandPartitionedTopicMetadata partitionMetadata) {
        partitionsMetadataRequests.inc();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received PartitionMetadataLookup", clientAddress);
        }
        final long clientRequestId = partitionMetadata.getRequestId();
        if (this.service.getLookupRequestSemaphore().tryAcquire()) {
            handlePartitionMetadataResponse(partitionMetadata, clientRequestId);
            this.service.getLookupRequestSemaphore().release();
        } else {
            rejectedPartitionsMetadataRequests.inc();
            if (log.isDebugEnabled()) {
                log.debug("PartitionMetaData Request ID {} from {} rejected - {}.", clientRequestId, clientAddress,
                        throttlingErrorMessage);
            }
            proxyConnection.ctx().writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.ServiceNotReady,
                    throttlingErrorMessage, clientRequestId));
        }
    }

    private void handlePartitionMetadataResponse(CommandPartitionedTopicMetadata partitionMetadata,
            long clientRequestId) {
        TopicName topicName = TopicName.get(partitionMetadata.getTopic());
        if (isBlank(brokerServiceURL)) {
            service.getDiscoveryProvider().getPartitionedTopicMetadata(service, topicName,
                    proxyConnection.clientAuthRole, proxyConnection.authenticationData).thenAccept(metadata -> {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Total number of partitions for topic {} is {}",
                                    proxyConnection.clientAuthRole, topicName, metadata.partitions);
                        }
                        proxyConnection.ctx().writeAndFlush(
                                Commands.newPartitionMetadataResponse(metadata.partitions, clientRequestId));
                    }).exceptionally(ex -> {
                        log.warn("[{}] Failed to get partitioned metadata for topic {} {}", clientAddress, topicName,
                                ex.getMessage(), ex);
                        proxyConnection.ctx().writeAndFlush(Commands.newPartitionMetadataResponse(
                                ServerError.ServiceNotReady, ex.getMessage(), clientRequestId));
                        return null;
                    });
        } else {
            URI brokerURI;
            try {
                brokerURI = new URI(brokerServiceURL);
            } catch (URISyntaxException e) {
                proxyConnection.ctx().writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.MetadataError,
                        e.getMessage(), clientRequestId));
                return;
            }
            InetSocketAddress addr = new InetSocketAddress(brokerURI.getHost(), brokerURI.getPort());

            if (log.isDebugEnabled()) {
                log.debug("Getting connections to '{}' for Looking up topic '{}' with clientReq Id '{}'", addr,
                        topicName.getPartitionedTopicName(), clientRequestId);
            }

            proxyConnection.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
                // Connected to backend broker
                long requestId = proxyConnection.newRequestId();
                ByteBuf command;
                command = Commands.newPartitionMetadataRequest(topicName.toString(), requestId);
                clientCnx.newLookup(command, requestId).thenAccept(lookupDataResult -> {
                    proxyConnection.ctx().writeAndFlush(
                            Commands.newPartitionMetadataResponse(lookupDataResult.partitions, clientRequestId));
                }).exceptionally((ex) -> {
                    log.warn("[{}] failed to get Partitioned metadata : {}", topicName.toString(),
                            ex.getCause().getMessage(), ex);
                    proxyConnection.ctx().writeAndFlush(Commands.newLookupErrorResponse(ServerError.ServiceNotReady,
                            ex.getMessage(), clientRequestId));
                    return null;
                });
            }).exceptionally(ex -> {
                // Failed to connect to backend broker
                proxyConnection.ctx().writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.ServiceNotReady,
                        ex.getMessage(), clientRequestId));
                return null;
            });
        }
    }

    public void handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace) {
        getTopicsOfNamespaceRequestss.inc();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received GetTopicsOfNamespace", clientAddress);
        }

        final long requestId = commandGetTopicsOfNamespace.getRequestId();

        if (this.service.getLookupRequestSemaphore().tryAcquire()) {
            handleGetTopicsOfNamespace(commandGetTopicsOfNamespace, requestId);
            this.service.getLookupRequestSemaphore().release();
        } else {
            rejectedGetTopicsOfNamespaceRequests.inc();
            if (log.isDebugEnabled()) {
                log.debug("GetTopicsOfNamespace Request ID {} from {} rejected - {}.", requestId, clientAddress,
                    throttlingErrorMessage);
            }
            proxyConnection.ctx().writeAndFlush(Commands.newError(
                requestId, ServerError.ServiceNotReady, throttlingErrorMessage
            ));
        }
    }

    private void handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace,
                                            long clientRequestId) {
        String serviceUrl = getServiceUrl(clientRequestId);

        if(!StringUtils.isNotBlank(serviceUrl)) {
            return;
        }
        performGetTopicsOfNamespace(clientRequestId, commandGetTopicsOfNamespace.getNamespace(), serviceUrl, 10,
            commandGetTopicsOfNamespace.getMode());
    }

    private void performGetTopicsOfNamespace(long clientRequestId,
                                             String namespaceName,
                                             String brokerServiceUrl,
                                             int numberOfRetries,
                                             CommandGetTopicsOfNamespace.Mode mode) {
        if (numberOfRetries == 0) {
            proxyConnection.ctx().writeAndFlush(Commands.newError(clientRequestId, ServerError.ServiceNotReady,
                    "Reached max number of redirections"));
            return;
        }

        InetSocketAddress addr = getAddr(brokerServiceUrl, clientRequestId);

        if(addr == null){
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Getting connections to '{}' for getting TopicsOfNamespace '{}' with clientReq Id '{}'",
                addr, namespaceName, clientRequestId);
        }
        proxyConnection.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
            // Connected to backend broker
            long requestId = proxyConnection.newRequestId();
            ByteBuf command;
            command = Commands.newGetTopicsOfNamespaceRequest(namespaceName, requestId, mode);
            clientCnx.newGetTopicsOfNamespace(command, requestId).thenAccept(topicList ->
                proxyConnection.ctx().writeAndFlush(
                    Commands.newGetTopicsOfNamespaceResponse(topicList, clientRequestId))
            ).exceptionally(ex -> {
                log.warn("[{}] Failed to get TopicsOfNamespace {}: {}", clientAddress, namespaceName, ex.getMessage());
                proxyConnection.ctx().writeAndFlush(
                        Commands.newError(clientRequestId, ServerError.ServiceNotReady, ex.getMessage()));
                return null;
            });
        }).exceptionally(ex -> {
            // Failed to connect to backend broker
            proxyConnection.ctx().writeAndFlush(
                    Commands.newError(clientRequestId, ServerError.ServiceNotReady, ex.getMessage()));
            return null;
        });
    }

    public void handleGetSchema(CommandGetSchema commandGetSchema) {
        getSchemaRequests.inc();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Received GetSchema", clientAddress);
        }

        final long clientRequestId = commandGetSchema.getRequestId();
        String serviceUrl = getServiceUrl(clientRequestId);

        if(!StringUtils.isNotBlank(serviceUrl)) {
            return;
        }
        InetSocketAddress addr = getAddr(serviceUrl, clientRequestId);

        if(addr == null){
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Getting connections to '{}' for getting schema of topic '{}' with clientReq Id '{}'",
                    addr, commandGetSchema.getTopic(), clientRequestId);
        }

        proxyConnection.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
            // Connected to backend broker
            long requestId = proxyConnection.newRequestId();
            ByteBuf command;
            byte[] schemaVersion = commandGetSchema.getSchemaVersion().toByteArray();
            command = Commands.newGetSchema(requestId, commandGetSchema.getTopic(),
                    Optional.ofNullable(BytesSchemaVersion.of(schemaVersion)));
            clientCnx.sendGetSchema(command, requestId).thenAccept(optionalSchemaInfo -> {
                        SchemaInfo schemaInfo = optionalSchemaInfo.get();
                        proxyConnection.ctx().writeAndFlush(
                                Commands.newGetSchemaResponse(clientRequestId,
                                        schemaInfo,
                                        BytesSchemaVersion.of(schemaVersion)));
            }).exceptionally(ex -> {
                log.warn("[{}] Failed to get schema {}: {}", clientAddress, commandGetSchema.getTopic(), ex.getMessage());
                proxyConnection.ctx().writeAndFlush(
                        Commands.newError(clientRequestId, ServerError.ServiceNotReady, ex.getMessage()));
                return null;
            });
        }).exceptionally(ex -> {
            // Failed to connect to backend broker
            proxyConnection.ctx().writeAndFlush(
                    Commands.newError(clientRequestId, ServerError.ServiceNotReady, ex.getMessage()));
            return null;
        });

    }

    private String getServiceUrl(long clientRequestId) {
        if (isBlank(brokerServiceURL)) {
            ServiceLookupData availableBroker;
            try {
                availableBroker = service.getDiscoveryProvider().nextBroker();
            } catch (Exception e) {
                log.warn("[{}] Failed to get next active broker {}", clientAddress, e.getMessage(), e);
                proxyConnection.ctx().writeAndFlush(Commands.newError(
                        clientRequestId, ServerError.ServiceNotReady, e.getMessage()
                ));
                return null;
            }
            return this.connectWithTLS ?
                    availableBroker.getPulsarServiceUrlTls() : availableBroker.getPulsarServiceUrl();
        } else {
            return this.connectWithTLS ?
                    service.getConfiguration().getBrokerServiceURLTLS() : service.getConfiguration().getBrokerServiceURL();
        }

    }

    private InetSocketAddress getAddr(String brokerServiceUrl, long clientRequestId) {
        URI brokerURI;
        try {
            brokerURI = new URI(brokerServiceUrl);
        } catch (URISyntaxException e) {
            proxyConnection.ctx().writeAndFlush(
                    Commands.newError(clientRequestId, ServerError.MetadataError, e.getMessage()));
            return null;
        }
        return InetSocketAddress.createUnresolved(brokerURI.getHost(), brokerURI.getPort());
    }

    private static final Logger log = LoggerFactory.getLogger(LookupProxyHandler.class);
}
