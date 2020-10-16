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
package org.apache.pulsar.broker.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.*;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ProduceMessageRequest;
import org.apache.pulsar.common.policies.data.ProduceMessageResponse;
import org.apache.pulsar.common.policies.data.RestProduceMessage;
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

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class TopicsBase extends PersistentTopicsBase {

    private ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<Integer>> owningTopics = new ConcurrentOpenHashMap<>();

    private SchemaRegistryService schemaRegistryService = pulsar().getSchemaRegistryService();

    private static String DEFAULT_PRODUCER_NAME = "RestProducer";

    protected  void publishMessages(AsyncResponse asyncResponse, ProduceMessageRequest request,
                                           boolean authoritative) {
        String topic = topicName.getPartitionedTopicName();
        if (owningTopics.containsKey(topic) || !findOwnerBrokerForTopic(authoritative, asyncResponse)) {
            // if we've done look up or or after look up this broker owns some of the partitions then proceed to publish message
            // else asyncResponse will be complete by look up.
            addOrGetSchemaForTopic(getSchemaData(request.getKeySchema(), request.getValueSchema()),
                new LongSchemaVersion(request.getSchemaVersion())).thenAccept(schemaMeta -> {
                    if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                        KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo =
                                KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaMeta.getLeft().toSchemaInfo());
                        publishMessagesToMultiplePartitions(topicName, request, owningTopics.get(topic), asyncResponse,
                                (KeyValueSchema) KeyValueSchema.of(AutoConsumeSchema.getSchema(kvSchemaInfo.getKey()),
                                        AutoConsumeSchema.getSchema(kvSchemaInfo.getValue())));
                    }else {
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
                if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                    KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaMeta.getLeft().toSchemaInfo());
                    publishMessagesToSinglePartition(topicName, request, partition, asyncResponse,
                            (KeyValueSchema) KeyValueSchema.of(AutoConsumeSchema.getSchema(kvSchemaInfo.getKey()),
                                    AutoConsumeSchema.getSchema(kvSchemaInfo.getValue())));
                } else {
                    asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to add or retrieve schema."));
                }
            });
        }

    }

    private CompletableFuture<PositionImpl> publishSingleMessageToPartition(String topic, Message message, String producerName) {
        CompletableFuture<PositionImpl> publishResult = new CompletableFuture<>();
        pulsar().getBrokerService().getTopic(topic, false).thenAccept(t -> {
            // TODO: Check message backlog
            if (!t.isPresent()) {
                // Topic not found, and remove from owning partition list.
                publishResult.completeExceptionally(new BrokerServiceException.TopicNotFoundException("Topic not owned by current broker."));
                TopicName topicName = TopicName.get(topic);
                owningTopics.get(topicName.getPartitionedTopicName()).remove(topicName.getPartitionIndex());
            } else {
                t.get().publishMessage(messageToByteBuf(message, producerName),
                        RestMessagePublishContext.get(publishResult, t.get(), System.nanoTime()));
            }
        });

        return publishResult;
    }

    private void publishMessagesToSinglePartition(TopicName topicName, ProduceMessageRequest request,
                                                  int partition, AsyncResponse asyncResponse,
                                                  KeyValueSchema keyValueSchema) {
        try {
            String producerName = request.getProducerName().isEmpty()? DEFAULT_PRODUCER_NAME : request.getProducerName();
            List<Message> messages = buildMessage(request, keyValueSchema);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults = new ArrayList<>();
            for (int index = 0; index < messages.size(); index++) {
                ProduceMessageResponse.ProduceMessageResult produceMessageResult = new ProduceMessageResponse.ProduceMessageResult();
                produceMessageResult.setPartition(partition);
                produceMessageResults.add(produceMessageResult);
                publishSingleMessageToPartition(topicName.getPartition(partition).toString(), messages.get(index),
                        producerName);
            }
            FutureUtil.waitForAll(publishResults);
            processPublishMessageResults(produceMessageResults, publishResults);
            asyncResponse.resume(new ProduceMessageResponse(produceMessageResults));
        } catch (JsonProcessingException e) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to deserialize messages to publish."));
        }
    }

    private void publishMessagesToMultiplePartitions(TopicName topicName, ProduceMessageRequest request,
                                                     ConcurrentOpenHashSet<Integer> partitionIndexes,
                                                     AsyncResponse asyncResponse, KeyValueSchema keyValueSchema) {
        try {
            String producerName = request.getProducerName().isEmpty()? DEFAULT_PRODUCER_NAME : request.getProducerName();
            List<Message> messages = buildMessage(request, keyValueSchema);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults = new ArrayList<>();
            List<Integer> owningPartitions = partitionIndexes.values();
            for (int index = 0; index < messages.size(); index++) {
                ProduceMessageResponse.ProduceMessageResult produceMessageResult = new ProduceMessageResponse.ProduceMessageResult();
                produceMessageResult.setPartition(owningPartitions.get(index % (int)partitionIndexes.size()));
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToPartition(topicName.getPartition(owningPartitions.get(index % (int)partitionIndexes.size())).toString(),
                    messages.get(index), producerName));
            }
            FutureUtil.waitForAll(publishResults);
            processPublishMessageResults(produceMessageResults, publishResults);
            asyncResponse.resume(new ProduceMessageResponse(produceMessageResults));
        } catch (JsonProcessingException e) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Fail to deserialize messages to publish."));
        }
    }

    private void processPublishMessageResults(List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults,
                                              List<CompletableFuture<PositionImpl>> publishResults) {
        // process publish message result
        for (int index = 0; index < produceMessageResults.size(); index++) {
            try {
                PositionImpl position = publishResults.get(index).get();
                produceMessageResults.get(index).setMessageId(position.toString());
                log.info("Successfully publish [{}] message with rest produce message request for topic  {}: {} ",
                        index, topicName, position);
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
        if (!(e instanceof BrokerServiceException.TopicFencedException || e instanceof ManagedLedgerException)) {
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

        if (!topicName.isPartitioned() && metadata.partitions > 1) {
            // Partitioned topic with multiple partitions, need to do look up for each partition.
            for (int index = 0; index < metadata.partitions; index++) {
                lookUpBrokerForTopic(topicName.getPartition(index), authoritative, redirectAddresses);
            }
        } else {
            // Non-partitioned topic or specific topic partition.
            lookUpBrokerForTopic(topicName, authoritative, redirectAddresses);
        }

        // Current broker doesn't own the topic or any partition of the topic, redirect client to a broker
        // that own partition of the topic or know who own partition of the topic.
        if (!owningTopics.containsKey(topicName.getPartitionedTopicName())) {
            if (redirectAddresses.isEmpty()) {
                // No broker to redirect, means look up for some partitions failed, client should retry with other brokers.
                asyncResponse.resume(new RestException(Status.NOT_FOUND, "Can't find owner of given topic."));
                return true;
            } else {
                // Redirect client to other broker owns the topic or know which broker own the topic.
                try {
                    URI redirectURI = new URI(String.format("%s%s", redirectAddresses.get(0), uri.getPath()));
                    asyncResponse.resume(Response.temporaryRedirect(redirectURI));
                    return true;
                } catch (URISyntaxException | NullPointerException e) {
                    log.error("Error in preparing redirect url with rest produce message request for topic  {}: {}",
                            topicName, e.getMessage(), e);
                    asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Fail to redirect client request."));
                    return true;
                }
            }
        }

        return false;
    }

    // Look up topic owner for non-partitioned topic or single topic partition.
    private void lookUpBrokerForTopic(TopicName partitionedTopicName, boolean authoritative, List<String> redirectAddresses) {
        CompletableFuture<Optional<LookupResult>> lookupFuture = pulsar().getNamespaceService()
                .getBrokerServiceUrlAsync(partitionedTopicName, LookupOptions.builder().authoritative(authoritative).loadTopicsInBundle(false).build());

        lookupFuture.thenAccept(optionalResult -> {
            if (optionalResult == null || !optionalResult.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug("Fail to lookup topic for rest produce message request for topic {}.",
                            partitionedTopicName);
                }
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses);
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
                completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses);
            } else {
                // Current broker doesn't own the topic or doesn't know who own the topic.
                if (log.isDebugEnabled()) {
                    log.debug("Complete topic look up for rest produce message request for topic {}, current broker is not owner broker: {}",
                            partitionedTopicName, result.getLookupData());
                }
                if (result.isRedirect()) {
                    // Redirect lookup.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), false), redirectAddresses);
                } else {
                    // Found owner for topic.
                    completeLookup(Pair.of(Arrays.asList(result.getLookupData().getHttpUrl(),
                            result.getLookupData().getHttpUrlTls()), true), redirectAddresses);
                }
            }
        }).exceptionally(exception -> {
            log.warn("Failed to lookup broker with rest produce message request for topic {}: {}",
                    partitionedTopicName, exception.getMessage(), exception);
            completeLookup(Pair.of(Collections.emptyList(), false), redirectAddresses);
            return null;
        });
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
                schemaAndMetadata = schemaRegistryService.getSchema(id, schemaVersion).get();
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
            pulsar().getBrokerService().getTopic(topicName.getPartition(index).toString(), false)
                .thenAccept(topic -> {
                    if (!topic.isPresent()) {
                        future.complete(null);
                    } else {
                        topic.get().addSchema(schemaData).thenAccept(schemaVersion -> future.complete(schemaVersion));
                    }
                });
            try {
                result.complete(future.get());
                break;
            } catch (InterruptedException | ExecutionException e) {
                // do nothing.
            }
        }
        if (!result.isDone()) {
            result.completeExceptionally(new SchemaException("Unable to add schema " + schemaData + " to topic " + topicName.getPartitionedTopicName()));
        }
        return result;
    }

    private SchemaVersion getSchemaVersion(CompletableFuture<SchemaVersion> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            return new LongSchemaVersion(-1);
        }
    }

    private SchemaData getSchemaData(String keySchema, String valueSchema) {
        try {
            if (keySchema == null && valueSchema == null) {
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
    public ByteBuf messageToByteBuf(Message message, String producerName) {
        checkArgument(message instanceof MessageImpl, "Message must be type of MessageImpl.");

        MessageImpl msg = (MessageImpl) message;
        PulsarApi.MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();

        // filled in required fields
        if (!msgMetadataBuilder.hasSequenceId()) {
            msgMetadataBuilder.setSequenceId(-1);
        }
        if (!msgMetadataBuilder.hasPublishTime()) {
            msgMetadataBuilder.setPublishTime(System.currentTimeMillis());
        }
        if (!msgMetadataBuilder.hasProducerName()) {
            msgMetadataBuilder.setProducerName(producerName);
        }

        msgMetadataBuilder.setCompression( CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        msgMetadataBuilder.setUncompressedSize(payload.readableBytes());
        PulsarApi.MessageMetadata msgMetadata = msgMetadataBuilder.build();

        ByteBuf byteBuf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, msgMetadata, payload);

        msgMetadataBuilder.recycle();
        msgMetadata.recycle();

        return byteBuf;
    }

    // Build pulsar message from serialized message.
    private List<Message> buildMessage(ProduceMessageRequest produceMessageRequest, KeyValueSchema keyValueSchema) throws JsonProcessingException {
        List<RestProduceMessage> messages;
        List<Message> pulsarMessages = new ArrayList<>();

        try {
            messages = ObjectMapperFactory.getThreadLocal().readValue(produceMessageRequest.getMessages(), new TypeReference<List<RestProduceMessage>>(){});
            for (RestProduceMessage message : messages) {
                PulsarApi.MessageMetadata.Builder metadataBuilder = PulsarApi.MessageMetadata.newBuilder();
                metadataBuilder.addAllReplicateTo(message.getReplicationClusters());
                metadataBuilder.setPartitionKey(keyValueSchema.getKeySchema().decode(Base64.getDecoder().decode(message.getKey())).toString());
                metadataBuilder.setEventTime(message.getEventTime());
                metadataBuilder.setSequenceId(message.getSequenceId());
                if (message.getDeliverAt() != 0) {
                    metadataBuilder.setDeliverAtTime(message.getDeliverAt());
                } else if (message.getDeliverAfterMs() != 0) {
                    metadataBuilder.setDeliverAtTime(message.getEventTime() + message.getDeliverAfterMs());
                }
                pulsarMessages.add(MessageImpl.create(metadataBuilder, ByteBuffer.wrap(message.getValue().getBytes(UTF_8)), keyValueSchema.getValueSchema()));
            }
        } catch (JsonProcessingException e) {
            if (log.isDebugEnabled()) {
                log.warn("Failed to deserialize message with rest produce message request for topic {}: {}",
                        topicName, produceMessageRequest.getMessages());
                throw e;
            }
        }

        return pulsarMessages;
    }

    private synchronized void completeLookup( Pair<List<String>, Boolean> result, List<String> redirectAddresses) {
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
    }
}
