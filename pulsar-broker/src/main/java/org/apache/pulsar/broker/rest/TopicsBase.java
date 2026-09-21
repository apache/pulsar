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
package org.apache.pulsar.broker.rest;

import static java.util.concurrent.TimeUnit.SECONDS;
import com.fasterxml.jackson.databind.ObjectReader;
import io.netty.buffer.ByteBuf;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.broker.web.RestProducerContext;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AvroBaseStructSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroWriter;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonWriter;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerAck;
import org.apache.pulsar.websocket.data.ProducerAcks;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;

/**
 * Contains methods used by REST api to producer/consumer/read messages to/from pulsar topics.
 */
public class TopicsBase extends PersistentTopicsBase {
    private static String defaultProducerName = "RestProducer";

    protected RestProducerContext restProducerContext() {
        return pulsar().getRestProducerContext();
    }

    // Publish message to a topic, can be partitioned or non-partitioned
    protected void publishMessages(AsyncResponse asyncResponse, ProducerMessages request, boolean authoritative) {
        String topic = topicName.getPartitionedTopicName();
        try {
            if (restProducerContext().isTopicOwned(topic)
                    || !findOwnerBrokerForTopic(topicName, authoritative, asyncResponse)) {
                // If we've done look up or or after look up this broker owns some of the partitions
                // then proceed to publish message else asyncResponse will be complete by look up.
                addOrGetSchemaForTopic(getSchemaData(request.getKeySchema(), request.getValueSchema()),
                        request.getSchemaVersion() == -1 ? null : new LongSchemaVersion(request.getSchemaVersion()))
                        .thenAccept(schemaMeta -> {
                            // Both schema version and schema data are necessary.
                            if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                                final var partitionIndexes = restProducerContext().getPartitionIndexes(topic);
                                internalPublishMessages(topicName, request, partitionIndexes, asyncResponse,
                                        AutoConsumeSchema.getSchema(schemaMeta.getLeft().toSchemaInfo()),
                                        schemaMeta.getRight());
                            } else {
                                asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR,
                                        "Fail to add or retrieve schema."));
                            }
                        }).exceptionally(e -> {
                            log.debug().exceptionMessage(e).log("Fail to publish message");
                            asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR,
                                    "Fail to publish message:"
                                            + e.getMessage()));
                            return null;
                        });
            }
        } catch (Exception e) {
            asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Fail to publish message: "
                    + e.getMessage()));
        }
    }

    // Publish message to single partition of a partitioned topic.
    protected void publishMessagesToPartition(AsyncResponse asyncResponse, ProducerMessages request,
                                                     boolean authoritative, int partition) {
        if (topicName.isPartitioned()) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, "Topic name can't contain "
                    + "'-partition-' suffix."));
        }
        String topic = topicName.getPartitionedTopicName();
        try {
            // If broker owns the partition then proceed to publish message, else do look up.
            if (restProducerContext().containsTopicPartition(topic, partition)
                    || !findOwnerBrokerForTopic(topicName.getPartition(partition), authoritative, asyncResponse)) {
                addOrGetSchemaForTopic(getSchemaData(request.getKeySchema(), request.getValueSchema()),
                        request.getSchemaVersion() == -1 ? null : new LongSchemaVersion(request.getSchemaVersion()))
                        .thenAccept(schemaMeta -> {
                            // Both schema version and schema data are necessary.
                            if (schemaMeta.getLeft() != null && schemaMeta.getRight() != null) {
                                internalPublishMessagesToPartition(topicName, request, partition, asyncResponse,
                                        AutoConsumeSchema.getSchema(schemaMeta.getLeft().toSchemaInfo()),
                                        schemaMeta.getRight());
                            } else {
                                asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR,
                                        "Fail to add or retrieve schema."));
                            }
                        }).exceptionally(e -> {
                        log.debug()
                                .attr("localizedMessage", e.getLocalizedMessage())
                                .log("Fail to publish message to single partition");
                                        asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR,
                                                "Fail to publish message"
                            + "to single partition: "
                            + e.getMessage()));
                    return null;
                });
            }
        } catch (Exception e) {
            asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Fail to publish message: "
                    + e.getMessage()));
        }
    }

    private void internalPublishMessagesToPartition(TopicName topicName, ProducerMessages request,
                                                  int partition, AsyncResponse asyncResponse,
                                                  Schema<?> schema, SchemaVersion schemaVersion) {
        try {
            String producerName = null == request.getProducerName() || request.getProducerName().isEmpty()
                    ? defaultProducerName : request.getProducerName();
            List<Message<?>> messages = buildMessage(request, schema, producerName, topicName, schemaVersion);
            List<CompletableFuture<Position>> publishResults = new ArrayList<>();
            List<ProducerAck> produceMessageResults = new ArrayList<>();
            for (int index = 0; index < messages.size(); index++) {
                ProducerAck produceMessageResult = new ProducerAck();
                produceMessageResult.setMessageId(partition + "");
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToPartition(topicName.getPartition(partition).toString(),
                        messages.get(index)));
            }
            FutureUtil.waitForAll(publishResults).thenRun(() -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
            }).exceptionally(e -> {
                // Some message may published successfully, so till return ok with result for each individual message.
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
                return null;
            });
        } catch (Exception e) {
                log.debug()
                        .attr("topic", topicName)
                        .exception(e.getCause())
                        .log("Fail publish messages to single partition with rest produce message request for topic");
                        asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage()));
        }
    }

    private void internalPublishMessages(TopicName topicName, ProducerMessages request,
                                                     List<Integer> partitionIndexes,
                                                     AsyncResponse asyncResponse, Schema<?> schema,
                                                     SchemaVersion schemaVersion) {
        if (partitionIndexes.isEmpty()) {
            asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR,
                    new BrokerServiceException.TopicNotFoundException("Topic not owned by current broker.")));
        }
        try {
            String producerName = null == request.getProducerName() || request.getProducerName().isEmpty()
                    ? defaultProducerName : request.getProducerName();
            List<Message<?>> messages = buildMessage(request, schema, producerName, topicName, schemaVersion);
            List<CompletableFuture<Position>> publishResults = new ArrayList<>();
            List<ProducerAck> produceMessageResults = new ArrayList<>();
            // Try to publish messages to all partitions this broker owns in round robin mode.
            for (int index = 0; index < messages.size(); index++) {
                ProducerAck produceMessageResult = new ProducerAck();
                produceMessageResult.setMessageId(partitionIndexes.get(index % partitionIndexes.size()) + "");
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishSingleMessageToPartition(topicName.getPartition(partitionIndexes
                                .get(index % partitionIndexes.size())).toString(),
                        messages.get(index)));
            }
            FutureUtil.waitForAll(publishResults).thenRun(() -> {
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
            }).exceptionally(e -> {
                // Some message may published successfully, so till return ok with result for each individual message.
                processPublishMessageResults(produceMessageResults, publishResults);
                asyncResponse.resume(Response.ok().entity(new ProducerAcks(produceMessageResults,
                        ((LongSchemaVersion) schemaVersion).getVersion())).build());
                return null;
            });
        } catch (Exception e) {
                log.debug()
                        .attr("topic", topicName)
                        .exceptionMessage(e)
                        .log("Fail to publish messages with rest produce message request");
                        asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage()));
        }
    }

    private CompletableFuture<Position> publishSingleMessageToPartition(String topic, Message<?> message) {
        CompletableFuture<Position> publishResult = new CompletableFuture<>();
        pulsar().getBrokerService().getTopic(topic, false)
                .thenAccept(t -> {
            // TODO: Check message backlog and fail if backlog too large.
            if (t.isEmpty()) {
                // Topic not found, and remove from owning partition list.
                publishResult.completeExceptionally(new BrokerServiceException.TopicNotFoundException("Topic not "
                        + "owned by current broker."));
            } else {
                try {
                    ByteBuf headersAndPayload = messageToByteBuf(message);
                    try {
                        Topic topicObj = t.get();
                        topicObj.publishMessage(headersAndPayload,
                                RestMessagePublishContext.get(publishResult, topicObj, System.nanoTime()));
                    } finally {
                        headersAndPayload.release();
                    }
                } catch (Exception e) {
                        log.debug()
                                .attr("topic", topicName)
                                .exceptionMessage(e)
                                .log("Fail to publish single messages to topic");
                                        publishResult.completeExceptionally(e);
                }
            }
        });

        return publishResult;
    }

    // Process results for all message publishing attempts
    private void processPublishMessageResults(List<ProducerAck> produceMessageResults,
                                              List<CompletableFuture<Position>> publishResults) {
        // process publish message result
        for (int index = 0; index < publishResults.size(); index++) {
            try {
                Position position = publishResults.get(index).get();
                MessageId messageId = new MessageIdImpl(position.getLedgerId(), position.getEntryId(),
                        Integer.parseInt(produceMessageResults.get(index).getMessageId()));
                produceMessageResults.get(index).setMessageId(messageId.toString());
            } catch (Exception e) {
                log.debug()
                        .attr("publish", index)
                        .attr("topic", topicName)
                        .log("Fail publish message with rest produce message request for topic");
                extractException(e, produceMessageResults.get(index));
            }
        }
    }

    // Return error code depends on exception we got indicating if client should retry with same broker.
    private static void extractException(Exception e, ProducerAck produceMessageResult) {
        if (!(e instanceof BrokerServiceException.TopicFencedException && e instanceof ManagedLedgerException)) {
            produceMessageResult.setErrorCode(2);
        } else {
            produceMessageResult.setErrorCode(1);
        }
        produceMessageResult.setErrorMsg(e.getMessage());
    }

    // Look up topic owner for given topic. Return if asyncResponse has been completed
    // which indicating redirect or exception.
    private boolean findOwnerBrokerForTopic(TopicName topicName, boolean authoritative, AsyncResponse asyncResponse) {
        PartitionedTopicMetadata metadata = internalGetPartitionedMetadata(authoritative, false);
        List<CompletableFuture<Optional<LookupResult>>> lookupFutures = new ArrayList<>();
        if (!topicName.isPartitioned() && metadata.partitions > 0) {
            // Partitioned topic with multiple partitions, need to do look up for each partition.
            for (int index = 0; index < metadata.partitions; index++) {
                lookupFutures.add(lookUpBrokerForTopic(topicName.getPartition(index), authoritative)
                        .exceptionally(e -> Optional.empty()));
            }
        } else {
            // Non-partitioned topic or specific topic partition.
            lookupFutures.add(lookUpBrokerForTopic(topicName, authoritative)
                    .exceptionally(e -> Optional.empty()));
        }

        try {
            FutureUtil.waitForAll(lookupFutures).get();
            Map<String, List<LookupResult>> resultsByBrokerId =
                lookupFutures.stream()
                    .map(CompletableFuture::join)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.groupingBy(result -> result.getLookupData().getBrokerId()));

            // if any partition is served by this broker, then return false so that producing happens on this broker.
            if (resultsByBrokerId.containsKey(pulsar().getBrokerId())) {
                return false;
            }

            // redirect to broker with most number of partitions
            List<LookupResult> lookupResults = resultsByBrokerId.entrySet().stream()
                    .max(Comparator.comparingInt(entry -> entry.getValue().size()))
                    .map(Map.Entry::getValue)
                    .orElse(null);
            if (lookupResults != null) {
                LookupResult lookupResult = lookupResults.get(0);
                URI redirectUri = lookupResult.toRedirectUri(uri.getRequestUri());
                asyncResponse.resume(Response.temporaryRedirect(redirectUri).build());
                return true;
            }
        } catch (Exception e) {
            log.debug()
                    .attr("topic", topicName.toString())
                    .log("Fail to lookup topic for rest produce message request for topic .");
            if (!asyncResponse.isDone()) {
                asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Internal error: "
                        + e.getMessage()));
            }
            return true;
        }
        return false;
    }

    // Look up topic owner for non-partitioned topic or single topic partition.
    private CompletableFuture<Optional<LookupResult>> lookUpBrokerForTopic(TopicName partitionedTopicName,
                                                                           boolean authoritative) {
        Semaphore lookupRequestSemaphore = pulsar().getBrokerService().getLookupRequestSemaphore();
        if (!lookupRequestSemaphore.tryAcquire()) {
            log.debug("Too many concurrent lookup request.");
            return FutureUtil.failedFuture(new BrokerServiceException.TooManyRequestsException("Too many "
                    + "concurrent lookup request"));
        }

        LookupOptions lookupOptions = LookupOptions.builder()
                .webServiceAdvertisedListenerName(getWebServiceListenerName())
                .authoritative(authoritative)
                .build();
        CompletableFuture<Optional<LookupResult>> lookupFuture = pulsar().getNamespaceService()
                .getBrokerServiceUrlAsync(partitionedTopicName, lookupOptions);

        // release semaphore when lookup is complete
        lookupFuture.whenComplete((result, exception) -> {
            lookupRequestSemaphore.release();
        });

        // Register the topic eagerly when this broker is the owner so that subsequent steps see it
        // in RestProducerContext without racing the asynchronous TopicEvent.LOAD listener notification.
        return lookupFuture.thenApply(optResult -> {
            if (optResult.isPresent()
                    && pulsar().getBrokerId().equals(optResult.get().getLookupData().getBrokerId())) {
                restProducerContext().addTopic(partitionedTopicName.toString());
            }
            return optResult;
        });
    }

    private CompletableFuture<Pair<SchemaData, SchemaVersion>> addOrGetSchemaForTopic(SchemaData schemaData,
                                                                                      LongSchemaVersion schemaVersion) {
        CompletableFuture<Pair<SchemaData, SchemaVersion>> future = new CompletableFuture<>();
        // If schema version presents try to fetch existing schema.
        if (null != schemaVersion) {
            String id = TopicName.get(topicName.getPartitionedTopicName()).getSchemaName();
            SchemaRegistry.SchemaAndMetadata schemaAndMetadata;
            try {
                schemaAndMetadata = pulsar().getSchemaRegistryService().getSchema(id, schemaVersion).get();
                future.complete(Pair.of(schemaAndMetadata.schema, schemaAndMetadata.version));
            } catch (Exception e) {
                    log.debug()
                            .attr("version", schemaVersion.getVersion())
                            .attr("topic", topicName)
                            .exceptionMessage(e)
                            .log("Fail to retrieve schema of version for topic");
                                future.completeExceptionally(e);
            }
        } else if (null != schemaData) {
            // Else try to add schema to topic.
            SchemaVersion sv;
            try {
                sv = addSchema(schemaData).get();
                future.complete(Pair.of(schemaData, sv));
            } catch (Exception e) {
                    log.debug()
                            .attr("schema", new String(schemaData.toSchemaInfo().getSchema()))
                            .attr("topic", topicName)
                            .exceptionMessage(e)
                            .log("Fail to add schema for topic");
                                future.completeExceptionally(e);
            }
        } else {
            // Indicating exception.
            future.complete(Pair.of(null, null));
        }
        return future;
    }

    // Add a new schema to schema registry for a topic
    private CompletableFuture<SchemaVersion> addSchema(SchemaData schemaData) {
        // Only need to add to first partition the broker owns since the schema id in schema registry are
        // same for all partitions which is the partitionedTopicName
        String topic1 = topicName.getPartitionedTopicName();
        List<Integer> partitions = restProducerContext().getPartitionIndexes(topic1);
        CompletableFuture<SchemaVersion> result = new CompletableFuture<>();
        for (int index = 0; index < partitions.size(); index++) {
            CompletableFuture<SchemaVersion> future = new CompletableFuture<>();
            String topicPartitionName = topicName.getPartition(partitions.get(index)).toString();
            pulsar().getBrokerService().getTopic(topicPartitionName, false)
            .thenAccept(topic -> {
                if (topic.isEmpty()) {
                    future.completeExceptionally(new BrokerServiceException.TopicNotFoundException(
                            "Topic " + topicPartitionName + " not found"));
                } else {
                    topic.get().addSchema(schemaData).thenAccept(schemaVersion -> future.complete(schemaVersion))
                    .exceptionally(exception -> {
                        future.completeExceptionally(exception);
                        return null;
                    });
                }
            });
            try {
                result.complete(future.get());
                break;
            } catch (Exception e) {
                    log.debug()
                            .attr("partitionedTopicName", topicName.getPartitionedTopicName())
                            .attr("value", partitions.get(index))
                            .log("Fail to add schema to topic for partition for REST produce request.");
                            }
        }
        // Not able to add schema to any partition
        if (!result.isDone()) {
            result.completeExceptionally(new SchemaException("Unable to add schema " + schemaData
                    + " to topic " + topicName.getPartitionedTopicName()));
        }
        return result;
    }

    private static final ObjectReader SCHEMA_INFO_READER =
            ObjectMapperFactory.getMapper().reader().forType(SchemaInfoImpl.class);

    // Build schemaData from passed in schema string.
    private SchemaData getSchemaData(String keySchema, String valueSchema) {
        try {
            SchemaInfoImpl valueSchemaInfo = valueSchema == null || valueSchema.isEmpty()
                    ? (SchemaInfoImpl) StringSchema.utf8().getSchemaInfo() :
                    SCHEMA_INFO_READER.readValue(valueSchema);
            if (null == valueSchemaInfo.getName()) {
                valueSchemaInfo.setName(valueSchemaInfo.getType().toString());
            }
            // Value schema only
            if (keySchema == null || keySchema.isEmpty()) {
                return SchemaData.builder()
                        .data(valueSchemaInfo.getSchema())
                        .isDeleted(false)
                        .user("Rest Producer")
                        .timestamp(System.currentTimeMillis())
                        .type(valueSchemaInfo.getType())
                        .props(valueSchemaInfo.getProperties())
                        .build();
            } else {
                // Key_Value schema
                SchemaInfoImpl keySchemaInfo = SCHEMA_INFO_READER.readValue(keySchema);
                if (null == keySchemaInfo.getName()) {
                    keySchemaInfo.setName(keySchemaInfo.getType().toString());
                }
                SchemaInfo schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo("KVSchema-"
                                + topicName.getPartitionedTopicName(),
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
            log.debug()
                    .attr("keySchema", keySchema)
                    .attr("valueSchema", valueSchema)
                    .exception(e)
                    .log("Failed to parse schema info for rest produce request");
            return null;
        }
    }

    // Convert message to ByteBuf
    public ByteBuf messageToByteBuf(Message<?> message) {
        checkArgument(message instanceof MessageImpl, "Message must be type of MessageImpl.");

        MessageImpl<?> msg = (MessageImpl<?>) message;
        MessageMetadata messageMetadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();
        messageMetadata.setCompression(CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        messageMetadata.setUncompressedSize(payload.readableBytes());

        return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, payload);
    }

    // Build pulsar message from REST request.
    private List<Message<?>> buildMessage(ProducerMessages producerMessages, Schema<?> schema,
                                       String producerName, TopicName topicName, SchemaVersion schemaVersion) {
        List<ProducerMessage> messages;
        List<Message<?>> pulsarMessages = new ArrayList<>();

        messages = producerMessages.getMessages();
        for (ProducerMessage message : messages) {
            MessageMetadata messageMetadata = new MessageMetadata();
            messageMetadata.setProducerName(producerName);
            messageMetadata.setPublishTime(System.currentTimeMillis());
            messageMetadata.setSequenceId(message.getSequenceId());
            messageMetadata.setSchemaVersion(schemaVersion.bytes());
            if (null != message.getReplicationClusters()) {
                messageMetadata.addAllReplicateTos(message.getReplicationClusters());
            }

            if (null != message.getProperties()) {
                messageMetadata.addAllProperties(message.getProperties().entrySet().stream().map(entry -> {
                    KeyValue keyValue = new KeyValue();
                    keyValue.setKey(entry.getKey());
                    keyValue.setValue(entry.getValue());
                    return keyValue;
                }).collect(Collectors.toList()));
            }
            if (null != message.getKey()) {
                // If has key schema, encode partition key, else use plain text.
                if (schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
                    KeyValueSchemaImpl<?, ?> kvSchema = (KeyValueSchemaImpl<?, ?>) schema;
                    messageMetadata.setPartitionKey(
                            Base64.getEncoder()
                                    .encodeToString(encodeWithSchema(message.getKey(), kvSchema.getKeySchema())));
                    messageMetadata.setPartitionKeyB64Encoded(true);
                } else {
                    messageMetadata.setPartitionKey(message.getKey());
                    messageMetadata.setPartitionKeyB64Encoded(false);
                }
            }
            if (null != message.getEventTime() && !message.getEventTime().isEmpty()) {
                messageMetadata.setEventTime(Long.parseLong(message.getEventTime()));
            }
            if (message.isDisableReplication()) {
                messageMetadata.clearReplicateTo();
                messageMetadata.addReplicateTo("__local__");
            }
            if (message.getDeliverAt() != 0 && messageMetadata.hasEventTime()) {
                messageMetadata.setDeliverAtTime(message.getDeliverAt());
            } else if (message.getDeliverAfterMs() != 0) {
                messageMetadata.setDeliverAtTime(messageMetadata.getEventTime() + message.getDeliverAfterMs());
            }
            if (schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
                KeyValueSchemaImpl<?, ?> kvSchema = (KeyValueSchemaImpl<?, ?>) schema;
                pulsarMessages.add(MessageImpl.create(messageMetadata,
                        ByteBuffer.wrap(
                                encodeWithSchema(message.getPayload(), kvSchema.getValueSchema())),
                        schema, topicName.toString()));
            } else {
                pulsarMessages.add(MessageImpl.create(messageMetadata,
                        ByteBuffer.wrap(encodeWithSchema(message.getPayload(), schema)), schema,
                        topicName.toString()));
            }
        }

        return pulsarMessages;
    }

    // Encode message with corresponding schema, do necessary conversion before encoding
    @SuppressWarnings("unchecked")
    private byte[] encodeWithSchema(String input, Schema<?> schemaParam) {
        Schema<Object> schema = (Schema<Object>) schemaParam;
        try {
            switch (schema.getSchemaInfo().getType()) {
                case INT8:
                    return schema.encode(Byte.parseByte(input));
                case INT16:
                    return schema.encode(Short.parseShort(input));
                case INT32:
                    return schema.encode(Integer.parseInt(input));
                case INT64:
                    return schema.encode(Long.parseLong(input));
                case STRING:
                    return schema.encode(input);
                case FLOAT:
                    return schema.encode(Float.parseFloat(input));
                case DOUBLE:
                    return schema.encode(Double.parseDouble(input));
                case BOOLEAN:
                    return schema.encode(Boolean.parseBoolean(input));
                case BYTES:
                    return schema.encode(input.getBytes());
                case DATE:
                    return schema.encode(DateFormat.getDateInstance().parse(input));
                case TIME:
                    return schema.encode(new Time(Long.parseLong(input)));
                case TIMESTAMP:
                    return schema.encode(new Timestamp(Long.parseLong(input)));
                case INSTANT:
                    return schema.encode(Instant.parse(input));
                case LOCAL_DATE:
                    return schema.encode(LocalDate.parse(input));
                case LOCAL_TIME:
                    return schema.encode(LocalTime.parse(input));
                case LOCAL_DATE_TIME:
                    return schema.encode(LocalDateTime.parse(input));
                case JSON:
                    GenericJsonWriter jsonWriter = new GenericJsonWriter();
                    return jsonWriter.write(new GenericJsonRecord(null, null,
                          ObjectMapperFactory.getMapper().reader().readTree(input), schema.getSchemaInfo()));
                case AVRO:
                    AvroBaseStructSchema<?> avroSchema = (AvroBaseStructSchema<?>) schema;
                    Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema.getAvroSchema(), input);
                    DatumReader<GenericData.Record> reader =
                            new GenericDatumReader<GenericData.Record>(avroSchema.getAvroSchema());
                    GenericRecord genericRecord = reader.read(null, decoder);
                    GenericAvroWriter avroWriter = new GenericAvroWriter(avroSchema.getAvroSchema());
                    return avroWriter.write(new GenericAvroRecord(null,
                            avroSchema.getAvroSchema(), null, genericRecord));
                case PROTOBUF_NATIVE:
                case KEY_VALUE:
                default:
                    throw new PulsarClientException.InvalidMessageException("");
            }
        } catch (Exception e) {
            log.debug()
                    .attr("value", input)
                    .attr("schema", new String(schema.getSchemaInfo().getSchema()))
                    .log("Fail to encode value with schema for rest produce request");
            return new byte[0];
        }
    }

    public void validateProducePermission() throws Exception {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
                && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                throw new RestException(Status.UNAUTHORIZED, "Need to authenticate to perform the request");
            }
            AuthenticationParameters authParams = authParams();
            boolean isAuthorized;
            try {
                isAuthorized = pulsar().getBrokerService().getAuthorizationService()
                        .allowTopicOperationAsync(topicName, TopicOperation.PRODUCE, authParams)
                        .get(config().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
            } catch (TimeoutException e) {
                log.warn()
                        .attr("timeoutSec", config().getMetadataStoreOperationTimeoutSeconds())
                        .attr("topic", topicName)
                        .log("Timeout while checking authorization");
                throw new RestException(Status.INTERNAL_SERVER_ERROR, "Time-out while checking authorization");
            } catch (Exception e) {
                log.warn()
                        .attr("role", authParams.getClientRole())
                        .attr("originalPrincipal", authParams.getOriginalPrincipal())
                        .attr("topic", topicName)
                        .exceptionMessage(e)
                        .log("Producer-client with Role - failed to get permissions for topic - .");
                throw new RestException(Status.INTERNAL_SERVER_ERROR, "Failed to get permissions");
            }

            if (!isAuthorized) {
                throw new RestException(Status.UNAUTHORIZED, "Unauthorized to produce to topic " + topicName);
            }
        }
    }

}
