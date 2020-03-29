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

import com.google.common.collect.Lists;

import io.netty.channel.EventLoopGroup;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.NotFoundException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.protocol.schema.SchemaInfoUtil;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpLookupService implements LookupService {

    private final HttpClient httpClient;
    private final boolean useTls;

    private static final String BasePathV1 = "lookup/v2/destination/";
    private static final String BasePathV2 = "lookup/v2/topic/";

    public HttpLookupService(ClientConfigurationData conf, EventLoopGroup eventLoopGroup)
            throws PulsarClientException {
        this.httpClient = new HttpClient(conf.getServiceUrl(), conf.getAuthentication(),
                eventLoopGroup, conf.isTlsAllowInsecureConnection(), conf.getTlsTrustCertsFilePath());
        this.useTls = conf.isUseTls();
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
    @SuppressWarnings("deprecation")
    public CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> getBroker(TopicName topicName) {
        String basePath = topicName.isV2() ? BasePathV2 : BasePathV1;

        return httpClient.get(basePath + topicName.getLookupName(), LookupData.class).thenCompose(lookupData -> {
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
                return CompletableFuture.completedFuture(Pair.of(brokerAddress, brokerAddress));
            } catch (Exception e) {
                // Failed to parse url
            	log.warn("[{}] Lookup Failed due to invalid url {}, {}", topicName, uri, e.getMessage());
                return FutureUtil.failedFuture(e);
            }
        });
    }

    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(TopicName topicName) {
        String format = topicName.isV2() ? "admin/v2/%s/partitions" : "admin/%s/partitions";
        return httpClient.get(String.format(format, topicName.getLookupName()) + "?checkAllowAutoCreation=true",
                PartitionedTopicMetadata.class);
    }

    public String getServiceUrl() {
    	return httpClient.getServiceUrl();
    }

    @Override
    public CompletableFuture<List<String>> getTopicsUnderNamespace(NamespaceName namespace, Mode mode) {
        CompletableFuture<List<String>> future = new CompletableFuture<>();

        String format = namespace.isV2()
            ? "admin/v2/namespaces/%s/topics?mode=%s" : "admin/namespaces/%s/destinations?mode=%s";
        httpClient
            .get(String.format(format, namespace, mode.toString()), String[].class)
            .thenAccept(topics -> {
                List<String> result = Lists.newArrayList();
                // do not keep partition part of topic name
                Arrays.asList(topics).forEach(topic -> {
                    String filtered = TopicName.get(topic).getPartitionedTopicName();
                    if (!result.contains(filtered)) {
                        result.add(filtered);
                    }
                });
                future.complete(result);})
            .exceptionally(ex -> {
                log.warn("Failed to getTopicsUnderNamespace namespace {} {}.", namespace, ex.getMessage());
                future.completeExceptionally(ex);
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
        CompletableFuture<Optional<SchemaInfo>> future = new CompletableFuture<>();

        String schemaName = topicName.getSchemaName();
        String path = String.format("admin/v2/schemas/%s/schema", schemaName);
        if (version != null) {
            path = String.format("admin/v2/schemas/%s/schema/%s",
                    schemaName,
                    ByteBuffer.wrap(version).getLong());
        }
        httpClient.get(path, GetSchemaResponse.class).thenAccept(response -> {
            future.complete(Optional.of(SchemaInfoUtil.newSchemaInfo(schemaName, response)));
        }).exceptionally(ex -> {
            if (ex.getCause() instanceof NotFoundException) {
                future.complete(Optional.empty());
            } else {
                log.warn("Failed to get schema for topic {} version {}",
                        topicName,
                        version != null ? Base64.getEncoder().encodeToString(version) : null,
                        ex.getCause());
                future.completeExceptionally(ex);
            }
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
