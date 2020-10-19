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
package org.apache.pulsar.broker.admin.impl;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.rest.RestMessagePublishContext;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.*;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ProduceMessageRequest;
import org.apache.pulsar.common.policies.data.ProduceMessageResponse;
import org.apache.pulsar.common.policies.data.ProduceMessageRequest.RestProduceMessage;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 *
 */
@Slf4j
public class TopicsBase extends PersistentTopicsBase {

    private ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<Integer>> owningTopics = new ConcurrentOpenHashMap<>();

    private static String DEFAULT_PRODUCER_NAME = "RestProducer";

    protected  void publishMessages(AsyncResponse asyncResponse, ProduceMessageRequest request,
                                           boolean authoritative) {
        String topic = topicName.getPartitionedTopicName();
        if (owningTopics.containsKey(topic) || !findOwnerBrokerForTopic(authoritative, asyncResponse)) {
            // if we've done look up or or after look up this broker owns some of the partitions then proceed to publish message
            // else asyncResponse will be complete by look up.
            addOrGetSchemaForTopic(getSchemaData(request.getKeySchema(), request.getValueSchema()),
                new LongSchemaVersion(request.getSchemaVersion())).thenAccept(schemaMeta -> {
                    // Both schema version and schema data are necessary.
                    if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo =
                                KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaMeta.getLeft().toSchemaInfo());
                        publishMessagesToMultiplePartitions(topicName, request, owningTopics.get(topic), asyncResponse,
                                (KeyValueSchema) KeyValueSchema.of(AutoConsumeSchema.getSchema(kvSchemaInfo.getKey()),
                                        AutoConsumeSchema.getSchema(kvSchemaInfo.getValue())), schemaMeta.getRight());
                    } else {
                        asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                    }
            });

        }
    }

    protected void publishMessagesToPartition(AsyncResponse asyncResponse, ProduceMessageRequest request,
                                                     boolean authoritative, int partition) {
        if (topicName.isPartitioned()) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Topic name can't contain '-partition-' suffix."));
        }
        String topic = topicName.getPartitionedTopicName();
        // If broker owns the partition then proceed to publish message, else do look up.
        if ((owningTopics.containsKey(topic) && owningTopics.get(topic).contains(partition)) || !findOwnerBrokerForTopic(authoritative, asyncResponse)) {
            addOrGetSchemaForTopic(getSchemaData(request.getKeySchema(), request.getValueSchema()),
                new LongSchemaVersion(request.getSchemaVersion())).thenAccept(schemaMeta -> {
                    // Both schema version and schema data are necessary.
                    if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaMeta.getLeft().toSchemaInfo());
                        publishMessagesToSinglePartition(topicName, request, partition, asyncResponse,
                                (KeyValueSchema) KeyValueSchema.of(AutoConsumeSchema.getSchema(kvSchemaInfo.getKey()),
                                        AutoConsumeSchema.getSchema(kvSchemaInfo.getValue())), schemaMeta.getRight());
                    } else {
                        asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                    }
            });
        }
    }

    private CompletableFuture<PositionImpl> publishSingleMessageToPartition(String topic, Message message) {
        CompletableFuture<PositionImpl> publishResult = new CompletableFuture<>();
        pulsar().getBrokerService().getTopic(topic, false).thenAccept(t -> {
            // TODO: Check message backlog
            if (!t.isPresent()) {
                // Topic not found, and remove from owning partition list.
                publishResult.completeExceptionally(new BrokerServiceException.TopicNotFoundException("Topic not owned by current broker."));
                TopicName topicName = TopicName.get(topic);
                owningTopics.get(topicName.getPartitionedTopicName()).remove(topicName.getPartitionIndex());
            } else {
                t.get().publishMessage(messageToByteBuf(message),
                RestMessagePublishContext.get(publishResult, t.get(), System.nanoTime()));
            }
        });

        return publishResult;
    }

    private void publishMessagesToSinglePartition(TopicName topicName, ProduceMessageRequest request,
                                                  int partition, AsyncResponse asyncResponse,
                                                  KeyValueSchema keyValueSchema, SchemaVersion schemaVersion) {
        try {
            String producerName = (null == request.getProducerName() || request.getProducerName().isEmpty())? DEFAULT_PRODUCER_NAME : request.getProducerName();
            List<Message> messages = buildMessage(request, keyValueSchema, producerName);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults = new ArrayList<>();
            for (int index = 0; index < messages.size(); index++) {
                ProduceMessageResponse.ProduceMessageResult produceMessageResult = new ProduceMessageResponse.ProduceMessageResult();
                produceMessageResult.setPartition(partition);
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToPartition(topicName.getPartition(partition).toString(), messages.get(index)));
            }
            FutureUtil.waitForAll(publishResults).thenRun(() -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProduceMessageResponse(produceMessageResults, ((LongSchemaVersion)schemaVersion).getVersion())).build());
            }).exceptionally(e -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProduceMessageResponse(produceMessageResults, ((LongSchemaVersion)schemaVersion).getVersion())).build());
                return null;
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail publish message with rest produce message request for topic  {}: {} ",
                        topicName, e.getCause());
            }
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, e.getMessage()));
        }
    }

    private void publishMessagesToMultiplePartitions(TopicName topicName, ProduceMessageRequest request,
                                                     ConcurrentOpenHashSet<Integer> partitionIndexes,
                                                     AsyncResponse asyncResponse, KeyValueSchema keyValueSchema,
                                                     SchemaVersion schemaVersion) {
        try {
            String producerName = (null == request.getProducerName() || request.getProducerName().isEmpty())? DEFAULT_PRODUCER_NAME : request.getProducerName();
            List<Message> messages = buildMessage(request, keyValueSchema, producerName);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults = new ArrayList<>();
            List<Integer> owningPartitions = partitionIndexes.values();
            for (int index = 0; index < messages.size(); index++) {
                ProduceMessageResponse.ProduceMessageResult produceMessageResult = new ProduceMessageResponse.ProduceMessageResult();
                produceMessageResult.setPartition(owningPartitions.get(index % (int)partitionIndexes.size()));
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToPartition(topicName.getPartition(owningPartitions.get(index % (int)partitionIndexes.size())).toString(),
                    messages.get(index)));
            }
            FutureUtil.waitForAll(publishResults).thenRun(() -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProduceMessageResponse(produceMessageResults, ((LongSchemaVersion)schemaVersion).getVersion())).build());
            }).exceptionally(e -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProduceMessageResponse(produceMessageResults, ((LongSchemaVersion)schemaVersion).getVersion())).build());
                return null;
            });
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail publish message with rest produce message request for topic  {}: {} ",
                        topicName, e.getCause());
            }
            e.printStackTrace();
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, e.getMessage()));
        }
    }

    private void processPublishMessageResults(List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults,
                                              List<CompletableFuture<PositionImpl>> publishResults) {
        // process publish message result
        for (int index = 0; index < publishResults.size(); index++) {
            try {
                PositionImpl position = publishResults.get(index).get();
                produceMessageResults.get(index).setMessageId(position.toString());
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.warn("Fail publish [{}] message with rest produce message request for topic  {}: {} ",
                            index, topicName);
                }
                if (e instanceof BrokerServiceException.TopicNotFoundException) {
                    // Topic ownership might changed, force to look up again.
                    owningTopics.remove(topicName.getPartitionedTopicName());
                }
                extractException(e, produceMessageResults.get(index));
            }
        }
    }

    private void extractException(Exception e, ProduceMessageResponse.ProduceMessageResult produceMessageResult) {
        if (!(e instanceof BrokerServiceException.TopicFencedException && e instanceof ManagedLedgerException)) {
            produceMessageResult.setErrorCode(2);
        } else {
            produceMessageResult.setErrorCode(1);
        }
        produceMessageResult.setError(e.getMessage());
    }

    // Look up topic owner for given topic.
    // Return if asyncResponse has been completed.
    private boolean findOwnerBrokerForTopic(boolean authoritative, AsyncResponse asyncResponse) {
        PartitionedTopicMetadata metadata = internalGetPartitionedMetadata(authoritative, false);
        List<String> redirectAddresses = Collections.synchronizedList(new ArrayList<>());
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        List<CompletableFuture<Void>> lookupFutures = new ArrayList<>();
        if (!topicName.isPartitioned() && metadata.partitions > 1) {
            // Partitioned topic with multiple partitions, need to do look up for each partition.
            for (int index = 0; index < metadata.partitions; index++) {
                lookupFutures.add(lookUpBrokerForTopic(topicName.getPartition(index), authoritative, redirectAddresses));
            }
        } else {
            // Non-partitioned topic or specific topic partition.
            lookupFutures.add(lookUpBrokerForTopic(topicName, authoritative, redirectAddresses));
        }

        // Current broker doesn't own the topic or any partition of the topic, redirect client to a broker
        // that own partition of the topic or know who own partition of the topic.
        FutureUtil.waitForAll(lookupFutures).thenRun(() -> {
            if (!owningTopics.containsKey(topicName.getPartitionedTopicName())) {
                if (redirectAddresses.isEmpty()) {
                    // No broker to redirect, means look up for some partitions failed, client should retry with other brokers.
                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Can't find owner of given topic."));
                    future.complete(true);
                } else {
                    // Redirect client to other broker owns the topic or know which broker own the topic.
                    try {
                        URI redirectURI = new URI(String.format("%s%s", redirectAddresses.get(0), uri.getPath()));
                        asyncResponse.resume(Response.temporaryRedirect(redirectURI));
                        future.complete(true);
                    } catch (URISyntaxException | NullPointerException e) {
                        log.error("Error in preparing redirect url with rest produce message request for topic  {}: {}",
                                topicName, e.getMessage(), e);
                        asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Fail to redirect client request."));
                        future.complete(true);
                    }
                }
            }
            future.complete(false);
        });
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            if (log.isDebugEnabled()) {
                log.debug("Fail to lookup topic for rest produce message request for topic {}.", topicName.toString());
            }
            return true;
        }
    }

    // Look up topic owner for non-partitioned topic or single topic partition.
    private CompletableFuture<Void> lookUpBrokerForTopic(TopicName partitionedTopicName, boolean authoritative, List<String> redirectAddresses) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<Optional<LookupResult>> lookupFuture = pulsar().getNamespaceService()
                .getBrokerServiceUrlAsync(partitionedTopicName, LookupOptions.builder().authoritative(authoritative).loadTopicsInBundle(false).build());

        lookupFuture.thenAccept(optionalResult -> {
            if (optionalResult == null || !optionalResult.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug("Fail to lookup topic for rest produce message request for topic {}.",
                            partitionedTopicName);
                }
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses, future);
                return;
            }

            LookupResult result = optionalResult.get();
            if (result.getLookupData().getHttpUrl().equals(pulsar().getWebServiceAddress())) {
                pulsar().getBrokerService().getLookupRequestSemaphore().release();
                // Current broker owns the topic, add to owning topic.
                if (log.isDebugEnabled()) {
                    log.debug("Complete topic look up for rest produce message request for topic {}, current broker is owner broker: {}",
                            partitionedTopicName, result.getLookupData());
                }
                owningTopics.computeIfAbsent(partitionedTopicName.getPartitionedTopicName(),
                        (key) -> new ConcurrentOpenHashSet<Integer>()).add(partitionedTopicName.getPartitionIndex());
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses, future);
            } else {
                // Current broker doesn't own the topic or doesn't know who own the topic.
                if (log.isDebugEnabled()) {
                    log.debug("Complete topic look up for rest produce message request for topic {}, current broker is not owner broker: {}",
                            partitionedTopicName, result.getLookupData());
                }
                if (result.isRedirect()) {
                    // Redirect lookup.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), false), redirectAddresses, future);
                } else {
                    // Found owner for topic.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), true), redirectAddresses, future);
                }
            }
        }).exceptionally(exception -> {
            log.warn("Failed to lookup broker with rest produce message request for topic {}: {}",
                    partitionedTopicName, exception.getMessage(), exception);
            completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses, future);
            return null;
        });
        return future;
    }

    private CompletableFuture<Pair<SchemaData, SchemaVersion>> addOrGetSchemaForTopic(SchemaData schemaData, SchemaVersion schemaVersion) {
        CompletableFuture<Pair<SchemaData, SchemaVersion>> future = new CompletableFuture<>();
        if (null != schemaData) {
            SchemaVersion sv;
            try {
                sv = addSchema(schemaData).get();
                future.complete(Pair.of(schemaData, sv));
            } catch (InterruptedException | ExecutionException e) {
                future.complete(Pair.of(null, null));
            }
        } else if (null != schemaVersion) {
            String id = TopicName.get(topicName.getPartitionedTopicName()).getSchemaName();
            SchemaRegistry.SchemaAndMetadata schemaAndMetadata;
            try {
                schemaAndMetadata = pulsar().getSchemaRegistryService().getSchema(id, schemaVersion).get();
                future.complete(Pair.of(schemaAndMetadata.schema, schemaAndMetadata.version));
            } catch (InterruptedException | ExecutionException e) {
                future.complete(Pair.of(null, null));
            }
        } else {
            future.complete(Pair.of(null, null));
        }
        return future;
    }

    private CompletableFuture<SchemaVersion> addSchema(SchemaData schemaData) {
        // Only need to add to first partition the broker owns since the schema id in schema registry are
        // same for all partitions which is the partitionedTopicName
        List<Integer> partitions = owningTopics.get(topicName.getPartitionedTopicName()).values();
        CompletableFuture<SchemaVersion> result = new CompletableFuture<>();
        for (int index = 0; index < partitions.size(); index++) {
            CompletableFuture<SchemaVersion> future = new CompletableFuture<>();
            String partitionTopicName = topicName.getPartition(partitions.get(index)).toString();
            pulsar().getBrokerService().getTopic(partitionTopicName, false)
                .thenAccept(topic -> {
                    if (!topic.isPresent()) {
                        future.completeExceptionally(new BrokerServiceException.TopicNotFoundException(
                                "Topic " + partitionTopicName + " not found"));
                    } else {
                        topic.get().addSchema(schemaData).thenAccept(schemaVersion -> future.complete(schemaVersion))
                        .exceptionally(exception -> {
                            exception.printStackTrace();
                            future.completeExceptionally(exception);
                            return null;
                        });
                    }
                });
            try {
                result.complete(future.get());
                break;
            } catch (Exception e) {
                result.completeExceptionally(new SchemaException("Unable to add schema " + schemaData + " to topic " + topicName.getPartitionedTopicName()));
            }
        }
        return result;
    }

    private SchemaData getSchemaData(String keySchema, String valueSchema) {
        try {
            if ((keySchema == null || keySchema.isEmpty())&& (valueSchema == null || valueSchema.isEmpty())) {
                return null;
            } else {
                SchemaInfo keySchemaInfo = (keySchema == null || keySchema.isEmpty())? StringSchema.utf8().getSchemaInfo():
                        ObjectMapperFactory.getThreadLocal().readValue(Base64.getDecoder().decode(keySchema), SchemaInfo.class);
                SchemaInfo valueSchemaInfo = (valueSchema == null || valueSchema.isEmpty())? StringSchema.utf8().getSchemaInfo():
                        ObjectMapperFactory.getThreadLocal().readValue(Base64.getDecoder().decode(valueSchema), SchemaInfo.class);
                SchemaInfo schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo("KVSchema-" + topicName.getPartitionedTopicName(),
                        keySchemaInfo, valueSchemaInfo,
                        KeyValueEncodingType.SEPARATED);
                return SchemaData.builder()
                        .data(schemaInfo.getSchema())
                        .isDeleted(false)
                        .user("Rest Producer")
                        .timestamp(System.currentTimeMillis())
                        .type(schemaInfo.getType())
                        .props(schemaInfo.getProperties())
                        .build();
            }
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.warn("Fail to parse schema info for rest produce request with key schema {} and value schema {}"
                        , keySchema, valueSchema);
            }
            return null;
        }
    }

    // Convert message to ByteBuf
    public ByteBuf messageToByteBuf(Message message) {
        checkArgument(message instanceof MessageImpl, "Message must be type of MessageImpl.");

        MessageImpl msg = (MessageImpl) message;
        PulsarApi.MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();
        msgMetadataBuilder.setCompression(CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        msgMetadataBuilder.setUncompressedSize(payload.readableBytes());

        ByteBuf byteBuf = null;
        PulsarApi.MessageMetadata msgMetadata = null;
        try {
            msgMetadata = msgMetadataBuilder.build();
            byteBuf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, msgMetadata, payload);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            msgMetadataBuilder.recycle();
            msgMetadata.recycle();
        }

        return byteBuf;
    }

    // Build pulsar message from serialized message.
    private List<Message> buildMessage(ProduceMessageRequest produceMessageRequest, KeyValueSchema keyValueSchema,
                                       String producerName) {
        List<RestProduceMessage> messages;
        List<Message> pulsarMessages = new ArrayList<>();

        messages = produceMessageRequest.getMessages();
        for (RestProduceMessage message : messages) {
            try {
            PulsarApi.MessageMetadata.Builder metadataBuilder = PulsarApi.MessageMetadata.newBuilder();
            metadataBuilder.setProducerName(producerName);
            metadataBuilder.setPublishTime(System.currentTimeMillis());
            metadataBuilder.setSequenceId(message.getSequenceId());
            if (null != message.getReplicationClusters()) {
                metadataBuilder.addAllReplicateTo(message.getReplicationClusters());
            }

            if (null != message.getProperties()) {
                metadataBuilder.addAllProperties(message.getProperties().entrySet().stream().map(entry ->
                    PulsarApi.KeyValue.newBuilder()
                            .setKey(entry.getKey())
                            .setValue(entry.getValue())
                            .build()
                ).collect(Collectors.toList()));
            }
            if (null != message.getKey()) {
                log.info(new String(Base64.getDecoder().decode(message.getKey())));
                metadataBuilder.setPartitionKey(keyValueSchema.getKeySchema().decode(Base64.getDecoder().decode(message.getKey())).toString());
            }
            if (0 != message.getEventTime()) {
                metadataBuilder.setEventTime(message.getEventTime());
            }
            if (message.isDisableReplication()) {
                metadataBuilder.clearReplicateTo();
                metadataBuilder.addReplicateTo("__local__");
            }
            if (message.getDeliverAt() != 0) {
                metadataBuilder.setDeliverAtTime(message.getDeliverAt());
            } else if (message.getDeliverAfterMs() != 0) {
                metadataBuilder.setDeliverAtTime(message.getEventTime() + message.getDeliverAfterMs());
            }
            pulsarMessages.add(MessageImpl.create(metadataBuilder, ByteBuffer.wrap(Base64.getDecoder().decode(message.getValue())), keyValueSchema.getValueSchema()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return pulsarMessages;
    }

    private synchronized void completeLookup( Pair<List<String>, Boolean> result, List<String> redirectAddresses,
                                              CompletableFuture<Void> future) {
        pulsar().getBrokerService().getLookupRequestSemaphore().release();
        if (!result.getLeft().isEmpty()) {
            if (result.getRight()) {
                // If address is for owner of topic partition, add to head and it'll have higher priority
                // compare to broker for look redirect.
                redirectAddresses.add(0, isRequestHttps() ? result.getLeft().get(1) : result.getLeft().get(0));
            } else {
                redirectAddresses.add(redirectAddresses.size(), isRequestHttps() ? result.getLeft().get(1) : result.getLeft().get(0));
            }
        }
        future.complete(null);
    }
}
