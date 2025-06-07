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

import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.common.Attributes;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.impl.metrics.LatencyHistogram;
import org.apache.pulsar.client.impl.schema.SchemaInfoUtil;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpLookupService implements LookupService {

    private final HttpClient httpClient;
    private final boolean useTls;
    private final String listenerName;

    private static final String BasePathV1 = "lookup/v2/destination/";
    private static final String BasePathV2 = "lookup/v2/topic/";

    private final LatencyHistogram histoGetBroker;
    private final LatencyHistogram histoGetTopicMetadata;
    private final LatencyHistogram histoGetSchema;
    private final LatencyHistogram histoListTopics;

    public HttpLookupService(InstrumentProvider instrumentProvider, ClientConfigurationData conf,
                             EventLoopGroup eventLoopGroup)
            throws PulsarClientException {
        this.httpClient = new HttpClient(conf, eventLoopGroup);
        this.useTls = conf.isUseTls();
        this.listenerName = conf.getListenerName();

        LatencyHistogram histo = instrumentProvider.newLatencyHistogram("pulsar.client.lookup.duration",
                "Duration of lookup operations", null,
                Attributes.builder().put("pulsar.lookup.transport-type", "http").build());
        histoGetBroker = histo.withAttributes(Attributes.builder().put("pulsar.lookup.type", "topic").build());
        histoGetTopicMetadata =
                histo.withAttributes(Attributes.builder().put("pulsar.lookup.type", "metadata").build());
        histoGetSchema = histo.withAttributes(Attributes.builder().put("pulsar.lookup.type", "schema").build());
        histoListTopics = histo.withAttributes(Attributes.builder().put("pulsar.lookup.type", "list-topics").build());
    }

    @Override
    public void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        httpClient.setServiceUrl(serviceUrl);
    }

    /**
     * Calls http-lookup api to find broker-service address which can serve a given topic.
     *
     * @param topicName topic-name
     * @return broker-socket-address that serves given topic
     */
    @Override
    @SuppressWarnings("deprecation")
    public CompletableFuture<LookupTopicResult> getBroker(TopicName topicName) {
        String basePath = topicName.isV2() ? BasePathV2 : BasePathV1;
        String path = basePath + topicName.getLookupName();
        path = StringUtils.isBlank(listenerName) ? path : path + "?listenerName=" + Codec.encode(listenerName);

        long startTime = System.nanoTime();
        CompletableFuture<LookupData> httpFuture = httpClient.get(path, LookupData.class);

        httpFuture.thenRun(() -> {
            histoGetBroker.recordSuccess(System.nanoTime() - startTime);
        }).exceptionally(x -> {
            histoGetBroker.recordFailure(System.nanoTime() - startTime);
            return null;
        });

        return httpFuture.thenCompose(lookupData -> {
            // Convert LookupData into as SocketAddress, handling exceptions
            URI uri = null;
            try {
                if (useTls) {
                    uri = new URI(lookupData.getBrokerUrlTls());
                } else {
                    String serviceUrl = lookupData.getBrokerUrl();
                    if (serviceUrl == null) {
                        serviceUrl = lookupData.getNativeUrl();
                    }
                    uri = new URI(serviceUrl);
                }

                InetSocketAddress brokerAddress = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());
                return CompletableFuture.completedFuture(new LookupTopicResult(brokerAddress, brokerAddress,
                        false /* HTTP lookups never use the proxy */));
            } catch (Exception e) {
                // Failed to parse url
                log.warn("[{}] Lookup Failed due to invalid url {}, {}", topicName, uri, e.getMessage());
                return FutureUtil.failedFuture(e);
            }
        });
    }

    /**
     * {@inheritDoc}
     * @param useFallbackForNonPIP344Brokers HttpLookupService ignores this parameter
     */
    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(
            TopicName topicName, boolean metadataAutoCreationEnabled, boolean useFallbackForNonPIP344Brokers) {
        long startTime = System.nanoTime();

        String format = topicName.isV2() ? "admin/v2/%s/partitions" : "admin/%s/partitions";
        CompletableFuture<PartitionedTopicMetadata> httpFuture =  httpClient.get(
                String.format(format, topicName.getLookupName()) + "?checkAllowAutoCreation="
                        + metadataAutoCreationEnabled,
                PartitionedTopicMetadata.class);

        httpFuture.thenRun(() -> {
            histoGetTopicMetadata.recordSuccess(System.nanoTime() - startTime);
        }).exceptionally(x -> {
            histoGetTopicMetadata.recordFailure(System.nanoTime() - startTime);
            return null;
        });

        return httpFuture;
    }

    @Override
    public String getServiceUrl() {
        return httpClient.getServiceUrl();
    }

    @Override
    public InetSocketAddress resolveHost() {
        return httpClient.resolveHost();
    }

    @Override
    public CompletableFuture<GetTopicsResult> getTopicsUnderNamespace(NamespaceName namespace, Mode mode,
                                                                      String topicsPattern, String topicsHash) {
        long startTime = System.nanoTime();

        CompletableFuture<GetTopicsResult> future = new CompletableFuture<>();

        String format = namespace.isV2()
            ? "admin/v2/namespaces/%s/topics?mode=%s" : "admin/namespaces/%s/destinations?mode=%s";
        httpClient
            .get(String.format(format, namespace, mode.toString()), String[].class)
            .thenAccept(topics -> {
                future.complete(new GetTopicsResult(topics));
            }).exceptionally(ex -> {
                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                log.warn("Failed to getTopicsUnderNamespace namespace {} {}.", namespace, cause.getMessage());
                future.completeExceptionally(cause);
                return null;
            });

        future.thenRun(() -> {
            histoListTopics.recordSuccess(System.nanoTime() - startTime);
        }).exceptionally(x -> {
            histoListTopics.recordFailure(System.nanoTime() - startTime);
            return null;
        });

        return future;
    }

    @Override
    public CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName) {
        return getSchema(topicName, null);
    }

    @Override
    public CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName, byte[] version) {
        long startTime = System.nanoTime();
        CompletableFuture<Optional<SchemaInfo>> future = new CompletableFuture<>();

        String schemaName = topicName.getSchemaName();
        String path = String.format("admin/v2/schemas/%s/schema", schemaName);
        if (version != null) {
            if (version.length == 0) {
                future.completeExceptionally(new SchemaSerializationException("Empty schema version"));
                return future;
            }
            path = String.format("admin/v2/schemas/%s/schema/%s",
                    schemaName,
                    ByteBuffer.wrap(version).getLong());
        }
        httpClient.get(path, GetSchemaResponse.class).thenAccept(response -> {
            if (response.getType() == SchemaType.KEY_VALUE) {
                SchemaData data = SchemaData
                        .builder()
                        .data(SchemaUtils.convertKeyValueDataStringToSchemaInfoSchema(
                                response.getData().getBytes(StandardCharsets.UTF_8)))
                        .type(response.getType())
                        .props(response.getProperties())
                        .build();
                future.complete(Optional.of(SchemaInfoUtil.newSchemaInfo(schemaName, data)));
            } else {
                future.complete(Optional.of(SchemaInfoUtil.newSchemaInfo(schemaName, response)));
            }
        }).exceptionally(ex -> {
            Throwable cause = FutureUtil.unwrapCompletionException(ex);
            if (cause instanceof NotFoundException) {
                future.complete(Optional.empty());
            } else {
                log.warn("Failed to get schema for topic {} version {}",
                        topicName,
                        version != null ? Base64.getEncoder().encodeToString(version) : null,
                        cause);
                future.completeExceptionally(cause);
            }
            return null;
        });

        future.thenRun(() -> {
            histoGetSchema.recordSuccess(System.nanoTime() - startTime);
        }).exceptionally(x -> {
            histoGetSchema.recordFailure(System.nanoTime() - startTime);
            return null;
        });
        return future;
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }

    private static final Logger log = LoggerFactory.getLogger(HttpLookupService.class);
}
