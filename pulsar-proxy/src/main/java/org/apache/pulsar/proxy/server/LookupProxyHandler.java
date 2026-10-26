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

import static org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterUtil.acquireDirectMemoryPermitsAndWriteAndFlush;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import io.prometheus.client.Counter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.topiclistlimit.TopicListMemoryLimiter;
import org.apache.pulsar.broker.topiclistlimit.TopicListSizeResultCache;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.BinaryProtoLookupService;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandGetSchema;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterImpl;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterUtil;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.NettyChannelUtil;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;

@CustomLog
public class LookupProxyHandler {
    private final String throttlingErrorMessage = "Too many concurrent lookup and partitionsMetadata requests";
    private final ProxyConnection proxyConnection;
    private final BrokerDiscoveryProvider discoveryProvider;
    private final boolean connectWithTLS;

    private final SocketAddress clientAddress;
    private final String brokerServiceURL;

    private static final Counter LOOKUP_REQUESTS = Counter
            .build("pulsar_proxy_lookup_requests", "Counter of topic lookup requests").create().register();

    private static final Counter PARTITIONS_METADATA_REQUESTS = Counter
            .build("pulsar_proxy_partitions_metadata_requests", "Counter of partitions metadata requests").create()
            .register();

    private static final Counter GET_TOPICS_OF_NAMESPACE_REQUESTS = Counter
            .build("pulsar_proxy_get_topics_of_namespace_requests", "Counter of getTopicsOfNamespace requests")
            .create()
            .register();

    private static final Counter GET_SCHEMA_REQUESTS = Counter
            .build("pulsar_proxy_get_schema_requests", "Counter of schema requests")
            .create()
            .register();

    static final Counter REJECTED_LOOKUP_REQUESTS = Counter.build("pulsar_proxy_rejected_lookup_requests",
            "Counter of topic lookup requests rejected due to throttling").create().register();

    static final Counter REJECTED_PARTITIONS_METADATA_REQUESTS = Counter
            .build("pulsar_proxy_rejected_partitions_metadata_requests",
                    "Counter of partitions metadata requests rejected due to throttling")
            .create().register();

    static final Counter REJECTED_GET_TOPICS_OF_NAMESPACE_REQUESTS = Counter
            .build("pulsar_proxy_rejected_get_topics_of_namespace_requests",
                    "Counter of getTopicsOfNamespace requests rejected due to throttling")
            .create().register();
    private final Semaphore lookupRequestSemaphore;
    private final AsyncDualMemoryLimiterImpl maxTopicListInFlightLimiter;
    private final TopicListSizeResultCache topicListSizeResultCache;

    public LookupProxyHandler(ProxyService proxy, ProxyConnection proxyConnection) {
        this.discoveryProvider = proxy.getDiscoveryProvider();
        this.lookupRequestSemaphore = proxy.getLookupRequestSemaphore();
        this.proxyConnection = proxyConnection;
        this.clientAddress = proxyConnection.clientAddress();
        this.connectWithTLS = proxy.getConfiguration().isTlsEnabledWithBroker();
        this.brokerServiceURL = this.connectWithTLS ? proxy.getConfiguration().getBrokerServiceURLTLS()
                : proxy.getConfiguration().getBrokerServiceURL();
        this.maxTopicListInFlightLimiter = proxy.getMaxTopicListInFlightLimiter();
        this.topicListSizeResultCache = proxy.getTopicListSizeResultCache();
    }

    public void handleLookup(CommandLookupTopic lookup) {
        log.debug()
                .attr("clientAddress", clientAddress)
                .log("Received Lookup from");
        long clientRequestId = lookup.getRequestId();
        if (lookupRequestSemaphore.tryAcquire()) {
            try {
                LOOKUP_REQUESTS.inc();
                String serviceUrl = getBrokerServiceUrl(lookupErrorResponse(clientRequestId));
                if (serviceUrl != null) {
                    performLookup(clientRequestId, lookup.getTopic(), serviceUrl, false, 10);
                }
            } finally {
                lookupRequestSemaphore.release();
            }
        } else {
            REJECTED_LOOKUP_REQUESTS.inc();
            log.debug()
                    .attr("clientRequestId", clientRequestId)
                    .attr("clientAddress", clientAddress)
                    .attr("throttlingErrorMessage", throttlingErrorMessage)
                    .log("Lookup Request ID from rejected");
            writeAndFlush(Commands.newLookupErrorResponse(ServerError.TooManyRequests,
                    throttlingErrorMessage, clientRequestId));
        }

    }

    private void performLookup(long clientRequestId, String topic, String brokerServiceUrl, boolean authoritative,
            int numberOfRetries) {
        if (numberOfRetries == 0) {
            writeAndFlush(Commands.newLookupErrorResponse(ServerError.ServiceNotReady,
                    "Reached max number of redirections", clientRequestId));
            return;
        }

        InetSocketAddress addr = getAddr(brokerServiceUrl, lookupErrorResponse(clientRequestId));
        if (addr == null) {
            return;
        }
        log.debug()
                .attr("addr", addr)
                .attr("topic", topic)
                .attr("clientRequestId", clientRequestId)
                .log("Getting connection for topic lookup");
        proxyConnection.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
            // Connected to backend broker
            long requestId = proxyConnection.newRequestId();
            ByteBuf command;
            command = Commands.newLookup(topic, authoritative, requestId);

            clientCnx.newLookup(command, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    log.warn()
                            .attr("clientAddress", clientAddress)
                            .attr("topic", topic)
                            .exceptionMessage(t)
                            .log("Failed to lookup topic");
                    writeAndFlush(
                        Commands.newLookupErrorResponse(getServerError(t), t.getMessage(), clientRequestId));
                } else {
                    String brokerUrl = resolveBrokerUrlFromLookupDataResult(r);
                    if (r.redirect) {
                        // Need to try the lookup again on a different broker
                        performLookup(clientRequestId, topic, brokerUrl, r.authoritative, numberOfRetries - 1);
                    } else {
                        // Reply the same address for both TLS non-TLS. The reason
                        // is that whether we use TLS
                        // and broker is independent of whether the client itself
                        // uses TLS, but we need to force the
                        // client
                        // to use the appropriate target broker (and port) when it
                        // will connect back.
                        log.debug()
                                .attr("addr", addr)
                                .attr("topic", topic)
                                .attr("clientRequestId", clientRequestId)
                                .attr("brokerUrl", brokerUrl)
                                .log("Successfully performed lookup");
                        writeAndFlush(Commands.newLookupResponse(brokerUrl, brokerUrl, true,
                            LookupType.Connect, clientRequestId, true /* this is coming from proxy */));
                    }
                }
                proxyConnection.getConnectionPool().releaseConnection(clientCnx);
            });
        }).exceptionally(ex -> {
            // Failed to connect to backend broker
            writeAndFlush(
                    Commands.newLookupErrorResponse(getServerError(ex), ex.getMessage(), clientRequestId));
            return null;
        });
    }

    protected String resolveBrokerUrlFromLookupDataResult(BinaryProtoLookupService.LookupDataResult r) {
        return connectWithTLS ? r.brokerUrlTls : r.brokerUrl;
    }

    public void handlePartitionMetadataResponse(CommandPartitionedTopicMetadata partitionMetadata) {
        PARTITIONS_METADATA_REQUESTS.inc();
        log.debug()
                .attr("clientAddress", clientAddress)
                .log("Received PartitionMetadataLookup");
        final long clientRequestId = partitionMetadata.getRequestId();
        if (lookupRequestSemaphore.tryAcquire()) {
            try {
                handlePartitionMetadataResponse(partitionMetadata, clientRequestId);
            } finally {
                lookupRequestSemaphore.release();
            }
        } else {
            REJECTED_PARTITIONS_METADATA_REQUESTS.inc();
            log.debug()
                    .attr("clientRequestId", clientRequestId)
                    .attr("clientAddress", clientAddress)
                    .attr("throttlingErrorMessage", throttlingErrorMessage)
                    .log("PartitionMetaData Request ID rejected");
            writeAndFlush(Commands.newPartitionMetadataResponse(ServerError.ServiceNotReady,
                    throttlingErrorMessage, clientRequestId));
        }
    }

    /**
     *   Always get partition metadata from broker service.
     *
     *
     **/
    private void handlePartitionMetadataResponse(CommandPartitionedTopicMetadata partitionMetadata,
            long clientRequestId) {
        String topicName = TopicName.toFullTopicName(partitionMetadata.getTopic());
        boolean metadataAutoCreationEnabled = partitionMetadata.isMetadataAutoCreationEnabled();
        BiConsumer<ServerError, String> onError = (error, message) ->
                writeAndFlush(Commands.newPartitionMetadataResponse(error, message, clientRequestId));

        String serviceUrl = getBrokerServiceUrl(onError);
        if (serviceUrl == null) {
            return;
        }
        InetSocketAddress addr = getAddr(serviceUrl, onError);
        if (addr == null) {
            return;
        }

        log.debug()
                .attr("addr", addr)
                .attr("topic", topicName)
                .attr("clientRequestId", clientRequestId)
                .log("Getting connection for partition metadata lookup");
        // The frontend connection can clear its pool while the backend request is pending.
        ConnectionPool connectionPool = proxyConnection.getConnectionPool();
        FutureUtil.supplySafely(() -> connectionPool.getConnection(addr)).whenComplete((clientCnx, connectionError) -> {
            if (connectionError != null) {
                writePartitionMetadataError(connectionError, clientRequestId, ServerError.ServiceNotReady);
                return;
            }
            FutureUtil.supplySafely(() -> {
                long requestId = proxyConnection.newRequestId();
                ByteBuf command = Commands.newPartitionMetadataRequest(topicName, requestId,
                        metadataAutoCreationEnabled);
                return clientCnx.newLookup(command, requestId);
            }).whenComplete((result, error) -> connectionPool.releaseConnection(clientCnx))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.warn()
                                    .attr("topic", topicName)
                                    .exception(error)
                                    .log("failed to get Partitioned");
                            writePartitionMetadataError(error, clientRequestId, ServerError.UnknownError);
                        } else {
                            writeAndFlush(Commands.newPartitionMetadataResponse(result.partitions, clientRequestId));
                        }
                    });
        });
    }

    private BiConsumer<ServerError, String> lookupErrorResponse(long clientRequestId) {
        return (error, message) -> writeAndFlush(Commands.newLookupErrorResponse(error, message, clientRequestId));
    }

    private void writePartitionMetadataError(Throwable error, long clientRequestId, ServerError fallbackError) {
        Throwable cause = FutureUtil.unwrapCompletionException(error);
        if (cause == null) {
            cause = error;
        }
        ServerError serverError;
        if (cause instanceof PulsarClientException.TimeoutException
                || cause instanceof PulsarClientException.ConnectException
                || cause instanceof PulsarClientException.NotConnectedException
                || cause instanceof PulsarClientException.AlreadyClosedException
                || cause instanceof TimeoutException
                || (cause instanceof IOException && !(cause instanceof PulsarClientException))) {
            serverError = ServerError.ServiceNotReady;
        } else if (cause instanceof PulsarClientException clientException) {
            serverError = ClientCnx.revertClientExToErrorCode(clientException);
            if (serverError == ServerError.UnknownError) {
                serverError = fallbackError;
            }
        } else {
            serverError = fallbackError;
        }
        writeAndFlush(Commands.newPartitionMetadataResponse(serverError, cause.getMessage(), clientRequestId));
    }

    public void handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace) {
        GET_TOPICS_OF_NAMESPACE_REQUESTS.inc();
        log.debug()
                .attr("clientAddress", clientAddress)
                .log("Received GetTopicsOfNamespace");

        final long requestId = commandGetTopicsOfNamespace.getRequestId();

        if (lookupRequestSemaphore.tryAcquire()) {
            try {
                handleGetTopicsOfNamespace(commandGetTopicsOfNamespace, requestId);
            } finally {
                lookupRequestSemaphore.release();
            }
        } else {
            REJECTED_GET_TOPICS_OF_NAMESPACE_REQUESTS.inc();
            log.debug()
                    .attr("requestId", requestId)
                    .attr("clientAddress", clientAddress)
                    .attr("throttlingErrorMessage", throttlingErrorMessage)
                    .log("GetTopicsOfNamespace Request ID rejected");
            writeAndFlush(Commands.newError(
                requestId, ServerError.ServiceNotReady, throttlingErrorMessage
            ));
        }
    }

    private void handleGetTopicsOfNamespace(CommandGetTopicsOfNamespace commandGetTopicsOfNamespace,
                                            long clientRequestId) {
        String serviceUrl = getBrokerServiceUrl(clientRequestId);

        if (!StringUtils.isNotBlank(serviceUrl)) {
            return;
        }
        String topicsPattern = commandGetTopicsOfNamespace.hasTopicsPattern()
                ? commandGetTopicsOfNamespace.getTopicsPattern() : null;
        String topicsHash = commandGetTopicsOfNamespace.hasTopicsHash()
                ? commandGetTopicsOfNamespace.getTopicsHash() : null;
        // The decoder reuses the command and releases its buffer once this handler returns, so the properties
        // must be copied before the connection future completes.
        Map<String, String> properties = new HashMap<>();
        for (KeyValue kv : commandGetTopicsOfNamespace.getPropertiesList()) {
            properties.put(kv.getKey(), kv.getValue());
        }
        performGetTopicsOfNamespace(clientRequestId, commandGetTopicsOfNamespace.getNamespace(), serviceUrl,
                10, topicsPattern, topicsHash, commandGetTopicsOfNamespace.getMode(), properties);
    }

    private void performGetTopicsOfNamespace(long clientRequestId,
                                             String namespaceName,
                                             String brokerServiceUrl,
                                             int numberOfRetries,
                                             String topicsPattern,
                                             String topicsHash,
                                             CommandGetTopicsOfNamespace.Mode mode,
                                             Map<String, String> properties) {
        if (numberOfRetries == 0) {
            writeAndFlush(Commands.newError(clientRequestId, ServerError.ServiceNotReady,
                    "Reached max number of redirections"));
            return;
        }

        InetSocketAddress addr = getAddr(brokerServiceUrl, clientRequestId);

        if (addr == null) {
            return;
        }

        log.debug()
                .attr("addr", addr)
                .attr("namespaceName", namespaceName)
                .attr("clientRequestId", clientRequestId)
                .log("Getting connection for TopicsOfNamespace");
        proxyConnection.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
            // Connected to backend broker
            long requestId = proxyConnection.newRequestId();
            ByteBuf command;
            command = Commands.newGetTopicsOfNamespaceRequest(namespaceName, requestId, mode,
                    topicsPattern, topicsHash, properties);

            internalPerformGetTopicsOfNamespace(clientRequestId, namespaceName, mode, properties, clientCnx,
                    command, requestId);
            proxyConnection.getConnectionPool().releaseConnection(clientCnx);
        }).exceptionally(ex -> {
            // Failed to connect to backend broker
            writeAndFlush(
                    Commands.newError(clientRequestId, getServerError(ex), ex.getMessage()));
            return null;
        });
    }

    private void internalPerformGetTopicsOfNamespace(long clientRequestId, String namespaceName,
                                                     CommandGetTopicsOfNamespace.Mode mode,
                                                     Map<String, String> properties, ClientCnx clientCnx,
                                                     ByteBuf command, long requestId) {
        BooleanSupplier isPermitRequestCancelled = () -> !proxyConnection.ctx().channel().isActive();
        TopicListSizeResultCache.ResultHolder
                listSizeHolder = topicListSizeResultCache.getTopicListSize(namespaceName, mode, properties);
        listSizeHolder.getSizeAsync().thenAccept(initialSize -> {
            maxTopicListInFlightLimiter.withAcquiredPermits(initialSize,
                    AsyncDualMemoryLimiter.LimitType.HEAP_MEMORY, isPermitRequestCancelled, initialPermits -> {
                        return clientCnx.newGetTopicsOfNamespace(command, requestId).handle((r, t) -> {
                            if (t != null) {
                                log.warn()
                                        .attr("clientAddress", clientAddress)
                                        .attr("namespaceName", namespaceName)
                                        .exceptionMessage(t)
                                        .log("Failed to get TopicsOfNamespace");
                                listSizeHolder.resetIfInitializing();
                                writeAndFlush(Commands.newError(clientRequestId, getTopicListServerError(t),
                                        t.getMessage()));
                                return CompletableFuture.completedFuture(null);
                            } else {
                                long actualSize = TopicListMemoryLimiter.estimateTopicListSize(
                                        r.getNonPartitionedOrPartitionTopics());
                                listSizeHolder.updateSize(actualSize);
                                return maxTopicListInFlightLimiter.withUpdatedPermits(initialPermits, actualSize,
                                        isPermitRequestCancelled, permits -> {
                                            return handleWritingGetTopicsResponse(clientRequestId, r,
                                                    isPermitRequestCancelled);
                                        }, t2 -> {
                                            if (!isTopicListRequestCancelled(t2,
                                                    "Failed to acquire actual heap memory permits for "
                                                            + "GetTopicsOfNamespace")) {
                                                writeTopicListPermitsError(clientRequestId, t2, true, "heap");
                                            }
                                            return CompletableFuture.completedFuture(null);
                                        });
                            }
                        });
                    }, t -> {
                        // the command wasn't sent to the broker, release it
                        ReferenceCountUtil.safeRelease(command);
                        listSizeHolder.resetIfInitializing();
                        if (!isTopicListRequestCancelled(t,
                                "Failed to acquire initial heap memory permits for GetTopicsOfNamespace")) {
                            writeTopicListPermitsError(clientRequestId, t, listSizeHolder.isMeasured(), "heap");
                        }
                        return CompletableFuture.completedFuture(null);
                    }).exceptionally(ex -> {
                listSizeHolder.resetIfInitializing();
                writeAndFlush(Commands.newError(clientRequestId, getServerError(ex), ex.getMessage()));
                return null;
            });
        });
    }

    private CompletableFuture<Void> handleWritingGetTopicsResponse(long clientRequestId, GetTopicsResult r,
                                                                   BooleanSupplier isCancelled) {
        BaseCommand responseCommand = Commands.newGetTopicsOfNamespaceResponseCommand(
                r.getNonPartitionedOrPartitionTopics(), r.getTopicsHash(), r.isFiltered(),
                r.isChanged(), clientRequestId);
        return acquireDirectMemoryPermitsAndWriteAndFlush(proxyConnection.ctx(), maxTopicListInFlightLimiter,
                isCancelled, responseCommand, t -> {
                    if (!isTopicListRequestCancelled(t,
                            "Failed to acquire actual direct memory permits for GetTopicsOfNamespace")) {
                        writeTopicListPermitsError(clientRequestId, t, true, "direct");
                    }
                    return CompletableFuture.completedFuture(null);
                });
    }

    /**
     * Responds to a GetTopicsOfNamespace request that couldn't acquire topic list memory permits. A topic list that
     * has been measured to be larger than the limit gets a non-retriable error, since a retry fails the same way
     * until the list is measured again. A permit timeout or a full queue, or an estimate larger than the limit for
     * a topic list that hasn't been measured, gets {@link ServerError#TooManyRequests}, which the client retries.
     */
    private void writeTopicListPermitsError(long clientRequestId, Throwable t, boolean topicListMeasured,
                                            String memoryType) {
        if (topicListMeasured && AsyncDualMemoryLimiterUtil.isPermitAcquireLimitExceeded(t)) {
            writeAndFlush(Commands.newError(clientRequestId, ServerError.NotAllowedError,
                    "Failed since the topic list is larger than the topic list " + memoryType + " memory limit"));
        } else {
            writeAndFlush(Commands.newError(clientRequestId, ServerError.TooManyRequests,
                    "Failed due to " + memoryType + " memory limit exceeded"));
        }
    }

    /**
     * Logs a failure to acquire topic list memory permits. A request that was cancelled, because the client
     * connection was closed, is logged at debug level, since there is no client to respond to.
     *
     * @return true if the request was cancelled and no error response should be sent
     */
    private boolean isTopicListRequestCancelled(Throwable t, String failureMessage) {
        if (AsyncDualMemoryLimiterUtil.isPermitAcquireCancelled(t)) {
            log.debug()
                    .attr("clientAddress", clientAddress)
                    .log("GetTopicsOfNamespace was cancelled since the client connection was closed");
            return true;
        }
        log.warn()
                .attr("clientAddress", clientAddress)
                .exceptionMessage(t)
                .log(failureMessage);
        return false;
    }

    public void handleGetSchema(CommandGetSchema commandGetSchema) {
        GET_SCHEMA_REQUESTS.inc();
        log.debug()
                .attr("clientAddress", clientAddress)
                .attr("commandGetSchema", commandGetSchema)
                .log("Received GetSchema");

        final long clientRequestId = commandGetSchema.getRequestId();
        String serviceUrl = getBrokerServiceUrl(clientRequestId);
        String topic = commandGetSchema.getTopic();
        Optional<SchemaVersion> schemaVersion;
        if (commandGetSchema.hasSchemaVersion()) {
            schemaVersion = Optional.of(commandGetSchema.getSchemaVersion()).map(BytesSchemaVersion::of);
        } else {
            schemaVersion = Optional.empty();
        }

        if (!StringUtils.isNotBlank(serviceUrl)) {
            return;
        }
        InetSocketAddress addr = getAddr(serviceUrl, clientRequestId);

        if (addr == null) {
            return;
        }
        log.debug()
                .attr("addr", addr)
                .attr("topic", topic)
                .attr("clientRequestId", clientRequestId)
                .log("Getting connection for schema of topic");

        proxyConnection.getConnectionPool().getConnection(addr).thenAccept(clientCnx -> {
            // Connected to backend broker
            long requestId = proxyConnection.newRequestId();
            ByteBuf command;
            command = Commands.newGetSchema(requestId, topic, schemaVersion);
            clientCnx.sendGetRawSchema(command, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    log.warn()
                            .attr("clientAddress", clientAddress)
                            .attr("topic", topic)
                            .attr("t", t)
                            .log("Failed to get schema");
                    writeAndFlush(
                        Commands.newError(clientRequestId, getServerError(t), t.getMessage()));
                } else {
                    writeAndFlush(
                        Commands.newGetSchemaResponse(clientRequestId, r));
                }

                proxyConnection.getConnectionPool().releaseConnection(clientCnx);
            });
        }).exceptionally(ex -> {
            // Failed to connect to backend broker
            writeAndFlush(
                    Commands.newError(clientRequestId, getServerError(ex), ex.getMessage()));
            return null;
        });

    }

    /**
     *  Get default broker service url or discovery an available broker.
     **/
    private String getBrokerServiceUrl(long clientRequestId) {
        return getBrokerServiceUrl((error, message) ->
                writeAndFlush(Commands.newError(clientRequestId, error, message)));
    }

    private String getBrokerServiceUrl(BiConsumer<ServerError, String> onError) {
        if (StringUtils.isNotBlank(brokerServiceURL)) {
            return brokerServiceURL;
        }
        ServiceLookupData availableBroker;
        try {
            availableBroker = discoveryProvider.nextBroker();
        } catch (Exception e) {
            log.warn()
                    .attr("clientAddress", clientAddress)
                    .attr("e", e.getMessage())
                    .exception(e)
                    .log("Failed to get next active broker");
            onError.accept(ServerError.ServiceNotReady, e.getMessage());
            return null;
        }
        String serviceUrl = availableBroker == null ? null
                : (connectWithTLS ? availableBroker.getPulsarServiceUrlTls() : availableBroker.getPulsarServiceUrl());
        if (StringUtils.isBlank(serviceUrl)) {
            onError.accept(ServerError.ServiceNotReady, "No broker service URL available");
            return null;
        }
        return serviceUrl;
    }

    private InetSocketAddress getAddr(String brokerServiceUrl, long clientRequestId) {
        return getAddr(brokerServiceUrl, (error, message) ->
                writeAndFlush(Commands.newError(clientRequestId, error, message)));
    }

    private InetSocketAddress getAddr(String brokerServiceUrl, BiConsumer<ServerError, String> onError) {
        if (StringUtils.isBlank(brokerServiceUrl)) {
            onError.accept(ServerError.ServiceNotReady, "No broker service URL available");
            return null;
        }
        try {
            URI brokerURI = new URI(brokerServiceUrl);
            if (StringUtils.isBlank(brokerURI.getHost()) || brokerURI.getPort() <= 0) {
                throw new IllegalArgumentException("Invalid broker service URL: " + brokerServiceUrl);
            }
            return InetSocketAddress.createUnresolved(brokerURI.getHost(), brokerURI.getPort());
        } catch (URISyntaxException | IllegalArgumentException e) {
            onError.accept(ServerError.MetadataError, e.getMessage());
            return null;
        }
    }

    private ServerError getServerError(Throwable error) {
        ServerError responseError;
        if (error instanceof PulsarClientException.AuthorizationException) {
            responseError = ServerError.AuthorizationError;
        } else if (error instanceof PulsarClientException.AuthenticationException) {
            responseError = ServerError.AuthenticationError;
        } else {
            responseError = ServerError.ServiceNotReady;
        }
        return responseError;
    }

    /**
     * Maps a broker's GetTopicsOfNamespace failure to the error relayed to the client. The broker rejects a topic
     * list that is larger than its topic list memory limit with {@link ServerError#NotAllowedError}, which is passed
     * through so that the client doesn't retry it.
     */
    private ServerError getTopicListServerError(Throwable error) {
        if (FutureUtil.unwrapCompletionException(error) instanceof PulsarClientException.NotAllowedException) {
            return ServerError.NotAllowedError;
        }
        return getServerError(error);
    }

    private void writeAndFlush(ByteBuf cmd) {
        final ChannelHandlerContext ctx = proxyConnection.ctx();
        NettyChannelUtil.writeAndFlushWithVoidPromise(ctx, cmd);
    }
}
