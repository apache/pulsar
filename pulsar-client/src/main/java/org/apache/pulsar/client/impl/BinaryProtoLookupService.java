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
package org.apache.pulsar.client.impl;

import static java.lang.String.format;
import static org.apache.pulsar.client.api.PulsarClientException.FailedFeatureCheck.SupportsGetPartitionedMetadataWithoutAutoCreation;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.common.Attributes;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.CustomLog;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.metrics.LatencyHistogram;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;
import org.jspecify.annotations.Nullable;

@CustomLog
public class BinaryProtoLookupService implements LookupService {

    private final PulsarClientImpl client;
    private final ServiceNameResolver serviceNameResolver;
    private final boolean useTls;
    private final ExecutorService scheduleExecutor;
    private final String listenerName;
    private final int maxLookupRedirects;
    private final ExecutorService lookupPinnedExecutor;
    private final boolean createdLookupPinnedExecutor;
    private final LatencyHistogram histoGetBroker;
    private final LatencyHistogram histoGetTopicMetadata;
    private final LatencyHistogram histoGetSchema;
    private final LatencyHistogram histoListTopics;

    /**
     * @deprecated use {@link
     * #BinaryProtoLookupService(PulsarClientImpl, String, String, boolean, ExecutorService, ExecutorService)} instead.
     */
    @Deprecated
    public BinaryProtoLookupService(PulsarClientImpl client,
                                    String serviceUrl,
                                    boolean useTls,
                                    ExecutorService scheduleExecutor)
            throws PulsarClientException {
        this(client, serviceUrl, null, useTls, scheduleExecutor);
    }

    /**
     * @deprecated use {@link
     * #BinaryProtoLookupService(PulsarClientImpl, String, String, boolean, ExecutorService, ExecutorService)} instead.
     */
    @Deprecated
    public BinaryProtoLookupService(PulsarClientImpl client,
                                    String serviceUrl,
                                    String listenerName,
                                    boolean useTls,
                                    ExecutorService scheduleExecutor)
            throws PulsarClientException {
        this(client, serviceUrl, listenerName, useTls, scheduleExecutor, null);
    }

    public BinaryProtoLookupService(PulsarClientImpl client,
                                    String serviceUrl,
                                    String listenerName,
                                    boolean useTls,
                                    ExecutorService scheduleExecutor,
                                    ExecutorService lookupPinnedExecutor)
            throws PulsarClientException {
        this.client = client;
        this.useTls = useTls;
        this.scheduleExecutor = scheduleExecutor;
        this.maxLookupRedirects = client.getConfiguration().getMaxLookupRedirects();
        this.serviceNameResolver =
                new PulsarServiceNameResolver(client.getConfiguration().getServiceUrlQuarantineInitDurationMs(),
                        client.getConfiguration().getServiceUrlQuarantineMaxDurationMs());
        this.listenerName = listenerName;
        updateServiceUrl(serviceUrl);

        LatencyHistogram histo = client.instrumentProvider().newLatencyHistogram("pulsar.client.lookup.duration",
                "Duration of lookup operations", null,
                Attributes.builder().put("pulsar.lookup.transport-type", "binary").build());
        histoGetBroker = histo.withAttributes(Attributes.builder().put("pulsar.lookup.type", "topic").build());
        histoGetTopicMetadata =
                histo.withAttributes(Attributes.builder().put("pulsar.lookup.type", "metadata").build());
        histoGetSchema = histo.withAttributes(Attributes.builder().put("pulsar.lookup.type", "schema").build());
        histoListTopics = histo.withAttributes(Attributes.builder().put("pulsar.lookup.type", "list-topics").build());

        if (lookupPinnedExecutor == null) {
            this.createdLookupPinnedExecutor = true;
            this.lookupPinnedExecutor =
                    Executors.newSingleThreadExecutor(new DefaultThreadFactory("pulsar-client-binary-proto-lookup"));
        } else {
            this.createdLookupPinnedExecutor = false;
            this.lookupPinnedExecutor = lookupPinnedExecutor;
        }
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
    public CompletableFuture<LookupTopicResult> getBroker(TopicName topicName, Map<String, String> lookupProperties) {
        if (lookupProperties == null) {
            lookupProperties = client.getConfiguration().getLookupProperties();
        }
        long startTime = System.nanoTime();
        CompletableFuture<LookupTopicResult> newFuture = findBroker(serviceNameResolver.resolveHost(), false,
                topicName, 0, lookupProperties);
        newFuture.thenRun(() -> {
            histoGetBroker.recordSuccess(System.nanoTime() - startTime);
        }).exceptionally(x -> {
            histoGetBroker.recordFailure(System.nanoTime() - startTime);
            return null;
        });
        return newFuture;
    }

    /**
     * calls broker binaryProto-lookup api to get metadata of partitioned-topic.
     *
     */
    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(
            TopicName topicName, boolean metadataAutoCreationEnabled, boolean useFallbackForNonPIP344Brokers) {
        return getPartitionedTopicMetadataAsync(topicName, metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
    }

    private CompletableFuture<LookupTopicResult> findBroker(InetSocketAddress socketAddress,
            boolean authoritative, TopicName topicName, final int redirectCount, Map<String, String> properties) {
        CompletableFuture<LookupTopicResult> addressFuture = new CompletableFuture<>();

        if (maxLookupRedirects > 0 && redirectCount > maxLookupRedirects) {
            addressFuture.completeExceptionally(
                    new PulsarClientException.LookupException("Too many redirects: " + maxLookupRedirects));
            return addressFuture;
        }

        client.getCnxPool().getConnection(socketAddress).thenAcceptAsync(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newLookup(topicName.toString(), listenerName, authoritative, requestId,
                    properties);
            clientCnx.newLookup(request, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    // lookup failed
                    log.warn().attr("topic", topicName).exceptionMessage(t).log("failed to send lookup request");
                        log.debug().attr("topic", topicName).exception(t).log("Lookup response exception");
                    addressFuture.completeExceptionally(t);
                } else {
                    URI uri = null;
                    try {
                        // (1) build response broker-address
                        if (useTls) {
                            uri = new URI(r.brokerUrlTls);
                        } else {
                            String serviceUrl = r.brokerUrl;
                            uri = new URI(serviceUrl);
                        }

                        InetSocketAddress responseBrokerAddress =
                                InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());

                        // (2) redirect to given address if response is: redirect
                        if (r.redirect) {
                            findBroker(responseBrokerAddress, r.authoritative, topicName, redirectCount + 1, properties)
                                .thenAccept(addressFuture::complete)
                                .exceptionally((lookupException) -> {
                                    Throwable cause = FutureUtil.unwrapCompletionException(lookupException);
                                    // lookup failed
                                    if (redirectCount > 0) {
                                            log.debug().attr("topic", topicName)
                                                    .attr("redirectCount", redirectCount)
                                                    .exceptionMessage(cause)
                                                    .log("lookup redirection failed");
                                    } else {
                                        log.warn().attr("topic", topicName)
                                                .exceptionMessage(cause)
                                                .exception(cause)
                                                .log("lookup failed");
                                    }
                                    addressFuture.completeExceptionally(cause);
                                    return null;
                            });
                        } else {
                            // (3) received correct broker to connect
                            if (r.proxyThroughServiceUrl) {
                                // Connect through proxy
                                addressFuture.complete(
                                        new LookupTopicResult(responseBrokerAddress, socketAddress, true));
                            } else {
                                // Normal result with direct connection to broker
                                addressFuture.complete(
                                        new LookupTopicResult(responseBrokerAddress, responseBrokerAddress, false));
                            }
                        }

                    } catch (Exception parseUrlException) {
                        // Failed to parse url
                        log.warn().attr("topicName", topicName)
                                .attr("url", uri)
                                .exceptionMessage(parseUrlException)
                                .exception(parseUrlException)
                                .log("invalid url");
                        addressFuture.completeExceptionally(parseUrlException);
                    }
                }
                client.getCnxPool().releaseConnection(clientCnx);
            });
        }, lookupPinnedExecutor).exceptionally(connectionException -> {
            serviceNameResolver.markHostAvailability(socketAddress, false);
            addressFuture.completeExceptionally(FutureUtil.unwrapCompletionException(connectionException));
            return null;
        });
        return addressFuture;
    }

    private CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(
            TopicName topicName, boolean metadataAutoCreationEnabled, boolean useFallbackForNonPIP344Brokers) {

        long startTime = System.nanoTime();
        CompletableFuture<PartitionedTopicMetadata> partitionFuture = new CompletableFuture<>();

        client.getCnxPool().getConnection(serviceNameResolver).thenAcceptAsync(clientCnx -> {
            boolean finalAutoCreationEnabled = metadataAutoCreationEnabled;
            if (!metadataAutoCreationEnabled && !clientCnx.isSupportsGetPartitionedMetadataWithoutAutoCreation()) {
                if (useFallbackForNonPIP344Brokers) {
                    log.info().attr("topicName", topicName)
                            .log("Using original behavior of"
                                    + " getPartitionedTopicMetadata(topic) in"
                                    + " getPartitionedTopicMetadata(topic,"
                                    + " false) since the target broker does"
                                    + " not support PIP-344 and fallback"
                                    + " is enabled.");
                    finalAutoCreationEnabled = true;
                } else {
                    partitionFuture.completeExceptionally(
                            new PulsarClientException.FeatureNotSupportedException("The feature of "
                                    + "getting partitions without auto-creation is not supported by the broker. "
                                    + "Please upgrade the broker to version that supports PIP-344 to resolve this "
                                    + "issue.",
                                    SupportsGetPartitionedMetadataWithoutAutoCreation));
                    return;
                }
            }
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newPartitionMetadataRequest(topicName.toString(), requestId,
                    finalAutoCreationEnabled);
            clientCnx.newLookup(request, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    histoGetTopicMetadata.recordFailure(System.nanoTime() - startTime);
                    log.warn().attr("topicName", topicName)
                            .exceptionMessage(t)
                            .exception(t)
                            .log("failed to get Partitioned metadata");
                    partitionFuture.completeExceptionally(t);
                } else {
                    try {
                        histoGetTopicMetadata.recordSuccess(System.nanoTime() - startTime);
                        partitionFuture.complete(new PartitionedTopicMetadata(r.partitions));
                    } catch (Exception e) {
                        partitionFuture.completeExceptionally(new PulsarClientException.LookupException(
                            format("Failed to parse partition-response redirect=%s, topic=%s, partitions with %s,"
                                            + " error message %s",
                                r.redirect, topicName, r.partitions,
                                e.getMessage())));
                    }
                }
                client.getCnxPool().releaseConnection(clientCnx);
            });
        }, lookupPinnedExecutor).exceptionally(connectionException -> {
            partitionFuture.completeExceptionally(FutureUtil.unwrapCompletionException(connectionException));
            return null;
        });

        return partitionFuture;
    }

    @Override
    public CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName, byte[] version) {
        long startTime = System.nanoTime();
        CompletableFuture<Optional<SchemaInfo>> schemaFuture = new CompletableFuture<>();
        if (version != null && version.length == 0) {
            schemaFuture.completeExceptionally(new SchemaSerializationException("Empty schema version"));
            return schemaFuture;
        }
        client.getCnxPool().getConnection(serviceNameResolver).thenAcceptAsync(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newGetSchema(requestId, topicName.toString(),
                Optional.ofNullable(BytesSchemaVersion.of(version)));
            clientCnx.sendGetSchema(request, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    histoGetSchema.recordFailure(System.nanoTime() - startTime);
                    log.warn().attr("topicName", topicName)
                            .exceptionMessage(t)
                            .exception(t)
                            .log("failed to get schema");
                    schemaFuture.completeExceptionally(t);
                } else {
                    histoGetSchema.recordSuccess(System.nanoTime() - startTime);
                    schemaFuture.complete(r);
                }
                client.getCnxPool().releaseConnection(clientCnx);
            });
        }, lookupPinnedExecutor).exceptionally(ex -> {
            schemaFuture.completeExceptionally(FutureUtil.unwrapCompletionException(ex));
            return null;
        });

        return schemaFuture;
    }

    public String getServiceUrl() {
        return serviceNameResolver.getServiceUrl();
    }

    @Override
    public InetSocketAddress resolveHost() {
        return serviceNameResolver.resolveHost();
    }

    @Override
    public CompletableFuture<GetTopicsResult> getTopicsUnderNamespace(NamespaceName namespace,
                                                                      Mode mode,
                                                                      String topicsPattern,
                                                                      String topicsHash,
                                                                      @Nullable Map<String, String> properties) {
        CompletableFuture<GetTopicsResult> topicsFuture = new CompletableFuture<>();
        AtomicLong opTimeoutMs = new AtomicLong(client.getConfiguration().getOperationTimeoutMs());
        Backoff backoff = Backoff.builder()
                .mandatoryStop(Duration.ofMillis(opTimeoutMs.get() * 2))
                .build();
        getTopicsUnderNamespace(namespace, backoff, opTimeoutMs, topicsFuture, mode,
                topicsPattern, topicsHash, properties);
        return topicsFuture;
    }

    @Override
    public boolean isBinaryProtoLookupService() {
        return true;
    }

    private void getTopicsUnderNamespace(
                                         NamespaceName namespace,
                                         Backoff backoff,
                                         AtomicLong remainingTime,
                                         CompletableFuture<GetTopicsResult> getTopicsResultFuture,
                                         Mode mode,
                                         String topicsPattern,
                                         String topicsHash,
                                         @Nullable Map<String, String> properties) {
        long startTime = System.nanoTime();

        client.getCnxPool().getConnection(serviceNameResolver).thenAcceptAsync(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newGetTopicsOfNamespaceRequest(
                namespace.toString(), requestId, mode, topicsPattern, topicsHash, properties);

            clientCnx.newGetTopicsOfNamespace(request, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    histoListTopics.recordFailure(System.nanoTime() - startTime);
                    getTopicsResultFuture.completeExceptionally(t);
                } else {
                    histoListTopics.recordSuccess(System.nanoTime() - startTime);
                        log.debug().attr("namespace", namespace)
                                .attr("request", requestId)
                                .log("[namespace: ] Success get topics list in request");
                    getTopicsResultFuture.complete(r);
                }
                client.getCnxPool().releaseConnection(clientCnx);
            });
        }, lookupPinnedExecutor).exceptionally((e) -> {
            long nextDelay = Math.min(backoff.next().toMillis(), remainingTime.get());
            if (nextDelay <= 0) {
                getTopicsResultFuture.completeExceptionally(
                    new PulsarClientException.TimeoutException(
                        format("Could not get topics of namespace %s within configured timeout",
                            namespace.toString())));
                return null;
            }

            ((ScheduledExecutorService) scheduleExecutor).schedule(() -> {
                log.warn().attr("namespace", namespace)
                        .attr("nextDelayMs", nextDelay)
                        .log("Could not get connection"
                                + " while getTopicsUnderNamespace"
                                + " -- Will try again later");
                remainingTime.addAndGet(-nextDelay);
                getTopicsUnderNamespace(namespace, backoff, remainingTime, getTopicsResultFuture,
                        mode, topicsPattern, topicsHash, properties);
            }, nextDelay, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    @Override
    public void close() throws Exception {
        if (createdLookupPinnedExecutor && lookupPinnedExecutor != null && !lookupPinnedExecutor.isShutdown()) {
            lookupPinnedExecutor.shutdown();
        }
    }

    public static class LookupDataResult {

        public final String brokerUrl;
        public final String brokerUrlTls;
        public final int partitions;
        public final boolean authoritative;
        public final boolean proxyThroughServiceUrl;
        public final boolean redirect;

        public LookupDataResult(CommandLookupTopicResponse result) {
            this.brokerUrl = result.hasBrokerServiceUrl() ? result.getBrokerServiceUrl() : null;
            this.brokerUrlTls = result.hasBrokerServiceUrlTls() ? result.getBrokerServiceUrlTls() : null;
            this.authoritative = result.isAuthoritative();
            this.redirect = result.hasResponse() && result.getResponse() == LookupType.Redirect;
            this.proxyThroughServiceUrl = result.isProxyThroughServiceUrl();
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
}
