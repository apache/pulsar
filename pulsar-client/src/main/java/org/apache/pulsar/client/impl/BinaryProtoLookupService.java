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
import io.netty.buffer.ByteBuf;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SchemaSerializationException;
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
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryProtoLookupService implements LookupService {

    private final PulsarClientImpl client;
    private final ServiceNameResolver serviceNameResolver;
    private final boolean useTls;
    private final ExecutorService executor;
    private final String listenerName;
    private final int maxLookupRedirects;

    public BinaryProtoLookupService(PulsarClientImpl client,
                                    String serviceUrl,
                                    boolean useTls,
                                    ExecutorService executor)
            throws PulsarClientException {
        this(client, serviceUrl, null, useTls, executor);
    }

    public BinaryProtoLookupService(PulsarClientImpl client,
                                    String serviceUrl,
                                    String listenerName,
                                    boolean useTls,
                                    ExecutorService executor)
            throws PulsarClientException {
        this.client = client;
        this.useTls = useTls;
        this.executor = executor;
        this.maxLookupRedirects = client.getConfiguration().getMaxLookupRedirects();
        this.serviceNameResolver = new PulsarServiceNameResolver();
        this.listenerName = listenerName;
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
        return findBroker(serviceNameResolver.resolveHost(), false, topicName, 0);
    }

    /**
     * calls broker binaryProto-lookup api to get metadata of partitioned-topic.
     *
     */
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(TopicName topicName) {
        return getPartitionedTopicMetadata(serviceNameResolver.resolveHost(), topicName);
    }

    private CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> findBroker(InetSocketAddress socketAddress,
            boolean authoritative, TopicName topicName, final int redirectCount) {
        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> addressFuture = new CompletableFuture<>();

        if (maxLookupRedirects > 0 && redirectCount > maxLookupRedirects) {
            addressFuture.completeExceptionally(
                    new PulsarClientException.LookupException("Too many redirects: " + maxLookupRedirects));
            return addressFuture;
        }

        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newLookup(topicName.toString(), listenerName, authoritative, requestId);
            clientCnx.newLookup(request, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    // lookup failed
                    log.warn("[{}] failed to send lookup request : {}", topicName, t.getMessage());
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Lookup response exception: {}", topicName, t);
                    }

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
                            findBroker(responseBrokerAddress, r.authoritative, topicName, redirectCount + 1)
                                .thenAccept(addressFuture::complete)
                                .exceptionally((lookupException) -> {
                                    Throwable cause = FutureUtil.unwrapCompletionException(lookupException);
                                    // lookup failed
                                    if (redirectCount > 0) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] lookup redirection failed ({}) : {}", topicName,
                                                    redirectCount, cause.getMessage());
                                        }
                                    } else {
                                        log.warn("[{}] lookup failed : {}", topicName,
                                                cause.getMessage(), cause);
                                    }
                                    addressFuture.completeExceptionally(cause);
                                    return null;
                            });
                        } else {
                            // (3) received correct broker to connect
                            if (r.proxyThroughServiceUrl) {
                                // Connect through proxy
                                addressFuture.complete(Pair.of(responseBrokerAddress, socketAddress));
                            } else {
                                // Normal result with direct connection to broker
                                addressFuture.complete(Pair.of(responseBrokerAddress, responseBrokerAddress));
                            }
                        }

                    } catch (Exception parseUrlException) {
                        // Failed to parse url
                        log.warn("[{}] invalid url {} : {}", topicName, uri, parseUrlException.getMessage(),
                            parseUrlException);
                        addressFuture.completeExceptionally(parseUrlException);
                    }
                }
                client.getCnxPool().releaseConnection(clientCnx);
            });
        }).exceptionally(connectionException -> {
            addressFuture.completeExceptionally(FutureUtil.unwrapCompletionException(connectionException));
            return null;
        });
        return addressFuture;
    }

    private CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(InetSocketAddress socketAddress,
            TopicName topicName) {

        CompletableFuture<PartitionedTopicMetadata> partitionFuture = new CompletableFuture<>();

        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newPartitionMetadataRequest(topicName.toString(), requestId);
            clientCnx.newLookup(request, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    log.warn("[{}] failed to get Partitioned metadata : {}", topicName,
                        t.getMessage(), t);
                    partitionFuture.completeExceptionally(t);
                } else {
                    try {
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
        }).exceptionally(connectionException -> {
            partitionFuture.completeExceptionally(FutureUtil.unwrapCompletionException(connectionException));
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
        CompletableFuture<Optional<SchemaInfo>> schemaFuture = new CompletableFuture<>();
        if (version != null && version.length == 0) {
            schemaFuture.completeExceptionally(new SchemaSerializationException("Empty schema version"));
            return schemaFuture;
        }
        InetSocketAddress socketAddress = serviceNameResolver.resolveHost();
        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newGetSchema(requestId, topicName.toString(),
                Optional.ofNullable(BytesSchemaVersion.of(version)));
            clientCnx.sendGetSchema(request, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    log.warn("[{}] failed to get schema : {}", topicName,
                        t.getMessage(), t);
                    schemaFuture.completeExceptionally(t);
                } else {
                    schemaFuture.complete(r);
                }
                client.getCnxPool().releaseConnection(clientCnx);
            });
        }).exceptionally(ex -> {
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
                                                                                  String topicsHash) {
        CompletableFuture<GetTopicsResult> topicsFuture = new CompletableFuture<>();

        AtomicLong opTimeoutMs = new AtomicLong(client.getConfiguration().getOperationTimeoutMs());
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMandatoryStop(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                .setMax(1, TimeUnit.MINUTES)
                .create();
        getTopicsUnderNamespace(serviceNameResolver.resolveHost(), namespace, backoff, opTimeoutMs, topicsFuture, mode,
                topicsPattern, topicsHash);
        return topicsFuture;
    }

    private void getTopicsUnderNamespace(InetSocketAddress socketAddress,
                                         NamespaceName namespace,
                                         Backoff backoff,
                                         AtomicLong remainingTime,
                                         CompletableFuture<GetTopicsResult> getTopicsResultFuture,
                                         Mode mode,
                                         String topicsPattern,
                                         String topicsHash) {
        client.getCnxPool().getConnection(socketAddress).thenAccept(clientCnx -> {
            long requestId = client.newRequestId();
            ByteBuf request = Commands.newGetTopicsOfNamespaceRequest(
                namespace.toString(), requestId, mode, topicsPattern, topicsHash);

            clientCnx.newGetTopicsOfNamespace(request, requestId).whenComplete((r, t) -> {
                if (t != null) {
                    getTopicsResultFuture.completeExceptionally(t);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[namespace: {}] Success get topics list in request: {}",
                                namespace, requestId);
                    }
                    // do not keep partition part of topic name
                    List<String> result = new ArrayList<>();
                    r.getTopics().forEach(topic -> {
                        String filtered = TopicName.get(topic).getPartitionedTopicName();
                        if (!result.contains(filtered)) {
                            result.add(filtered);
                        }
                    });

                    getTopicsResultFuture.complete(new GetTopicsResult(result, r.getTopicsHash(),
                            r.isFiltered(), r.isChanged()));
                }
                client.getCnxPool().releaseConnection(clientCnx);
            });
        }).exceptionally((e) -> {
            long nextDelay = Math.min(backoff.next(), remainingTime.get());
            if (nextDelay <= 0) {
                getTopicsResultFuture.completeExceptionally(
                    new PulsarClientException.TimeoutException(
                        format("Could not get topics of namespace %s within configured timeout",
                            namespace.toString())));
                return null;
            }

            ((ScheduledExecutorService) executor).schedule(() -> {
                log.warn("[namespace: {}] Could not get connection while getTopicsUnderNamespace -- Will try again in"
                                + " {} ms", namespace, nextDelay);
                remainingTime.addAndGet(-nextDelay);
                getTopicsUnderNamespace(socketAddress, namespace, backoff, remainingTime, getTopicsResultFuture,
                        mode, topicsPattern, topicsHash);
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

    private static final Logger log = LoggerFactory.getLogger(BinaryProtoLookupService.class);
}
