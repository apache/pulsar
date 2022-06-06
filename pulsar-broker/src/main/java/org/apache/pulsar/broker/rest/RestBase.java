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

import static org.apache.pulsar.client.impl.auth.AuthenticationDataToken.HTTP_HEADER_NAME;
import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
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
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
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
@Slf4j
public class RestBase extends PersistentTopicsBase {

    static final String HTTP_HEADER_VALUE_PREFIX = "Bearer ";

    protected CompletableFuture<ProducerAcks> publishMessagesByProducer(ProducerMessages request,
                                                                        boolean authoritative) {
        return getPublishTopicName(authoritative)
                .thenCompose(name -> checkTopicOwnershipAsync(name, authoritative))
                .thenCompose(__ -> checkProducePermissionAsync())
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(topicName.toString()))
                .thenCompose(op -> {
                    if (op.isPresent()) {
                        Optional<LongSchemaVersion> schemaVersion = request.getSchemaVersion() == -1
                                ? Optional.empty()
                                : Optional.of(new LongSchemaVersion(request.getSchemaVersion()));
                        Optional<SchemaData> schemaData = getSchemaData(request.getKeySchema(),
                                request.getValueSchema());
                        return getOrAddSchema(op.get(), schemaData, schemaVersion)
                                .thenCompose(schemaInfo -> publishMessages(request, schemaInfo));
                    } else {
                        return FutureUtil.failedFuture(new BrokerServiceException
                                .TopicNotFoundException(String.format("Topic %s not found", topicName.toString())));
                    }
                });
    }

    private CompletableFuture<ProducerAcks> publishMessages(ProducerMessages request,
                                                            SchemaAndMetadata schemaMetadata) {
        CompletableFuture<Producer<byte[]>> producerFuture;
        PulsarClient pulsarClient;
        try {
            pulsarClient = getPulsarClient();
            ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer();
            producerBuilder.topic(topicName.getPartitionedTopicName());
            producerBuilder.producerName(getProducerName(request));
            producerBuilder.enableBatching(false);
            producerFuture = producerBuilder.createAsync();
        } catch (Exception e) {
            return FutureUtil.failedFuture(new RuntimeException(e));
        }
        return producerFuture.thenApply(producer -> {
            Schema<?> schema = AutoConsumeSchema.getSchema(schemaMetadata.schema.toSchemaInfo());
            List<Message> messages = buildMessage(request, schema, topicName, false);
            List<CompletableFuture<MessageId>> results = new ArrayList<>();
            for (Message msg : messages) {
                results.add(((ProducerBase) producer).sendAsync(msg));
            }
            return results;
        }).thenCompose(futures -> FutureUtil.waitForAll(futures).thenApply(__ -> {
            List<ProducerAck> producerAckResults = new ArrayList<>();
            for (int index = 0; index < futures.size(); index++) {
                ProducerAck producerAck = new ProducerAck();
                MessageId messageId = futures.get(index).join();
                producerAck.setMessageId(messageId.toString());
                producerAckResults.add(producerAck);
            }
            producerFuture.join().closeAsync().whenComplete((v, ex) -> {
                pulsarClient.closeAsync();
            });
            return new ProducerAcks(producerAckResults, ((LongSchemaVersion) schemaMetadata.version).getVersion());
        }));
    }

    private PulsarClient getPulsarClient() throws Exception {
        ClientBuilder builder = PulsarClient.builder();
        builder.serviceUrl(config().isTlsEnabled()
                ? pulsar().getBrokerServiceUrlTls() : this.pulsar().getBrokerServiceUrl());
        String token = null;
        AuthenticationDataSource authenticationDataSource = clientAuthData();
        if (authenticationDataSource != null && authenticationDataSource.hasDataFromHttp()) {
            String httpHeaderValue = authenticationDataSource.getHttpHeader(HTTP_HEADER_NAME);
            if (httpHeaderValue != null && httpHeaderValue.startsWith(HTTP_HEADER_VALUE_PREFIX)) {
                token = httpHeaderValue.substring(HTTP_HEADER_VALUE_PREFIX.length());
            }
        }
        if (token != null) {
            Set<String> authenticationProviders = config().getAuthenticationProviders();
            if (authenticationProviders.contains(AuthenticationProviderToken.class.getName())) {
                builder.authentication(new AuthenticationToken(token));
            }
        }
        return builder.build();
    }

    protected CompletableFuture<ProducerAcks> publishMessages(ProducerMessages request, boolean authoritative) {
        return getPublishTopicName(authoritative)
                .thenCompose(name -> checkTopicOwnershipAsync(name, authoritative))
                .thenCompose(__ -> checkProducePermissionAsync())
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(topicName.toString()))
                .thenCompose(op -> {
                    if (op.isPresent()) {
                        Optional<LongSchemaVersion> schemaVersion = request.getSchemaVersion() == -1
                                ? Optional.empty()
                                : Optional.of(new LongSchemaVersion(request.getSchemaVersion()));
                        Optional<SchemaData> schemaData = getSchemaData(request.getKeySchema(),
                                request.getValueSchema());
                        return getOrAddSchema(op.get(), schemaData, schemaVersion)
                                .thenCompose(schemaInfo -> {
                                    Schema<?> schema = AutoConsumeSchema.getSchema(schemaInfo.schema.toSchemaInfo());
                                    return publishMessages(request, op.get(), schema, schemaInfo.version);
                                });
                    } else {
                        return FutureUtil.failedFuture(new BrokerServiceException
                                .TopicNotFoundException(String.format("Topic %s not found", topicName.toString())));
                    }
                });
    }

    protected CompletableFuture<ProducerAcks> publishMessages(ProducerMessages request, int partition,
                                                              boolean authoritative) {
        return getPublishTopicName(partition, authoritative)
                .thenCompose(name -> checkTopicOwnershipAsync(name, authoritative))
                .thenCompose(__ -> checkProducePermissionAsync())
                .thenCompose(__ -> pulsar().getBrokerService().getTopicIfExists(topicName.toString()))
                .thenCompose(op -> {
                    if (op.isPresent()) {
                        Optional<LongSchemaVersion> schemaVersion = request.getSchemaVersion() == -1
                                ? Optional.empty()
                                : Optional.of(new LongSchemaVersion(request.getSchemaVersion()));
                        Optional<SchemaData> schemaData = getSchemaData(request.getKeySchema(),
                                request.getValueSchema());
                        return getOrAddSchema(op.get(), schemaData, schemaVersion)
                                .thenCompose(schemaInfo -> {
                                    Schema<?> schema = AutoConsumeSchema.getSchema(schemaInfo.schema.toSchemaInfo());
                                    return publishMessages(request, op.get(), schema, schemaInfo.version);
                                });
                    } else {
                        return FutureUtil.failedFuture(new BrokerServiceException
                                .TopicNotFoundException(String.format("Topic %s not found", topicName.toString())));
                    }
                });
    }

    private CompletableFuture<TopicName> getPublishTopicName(boolean authoritative) {
        if (topicName.isPartitioned()) {
            return CompletableFuture.completedFuture(topicName);
        }
        return internalGetPartitionedMetadataAsync(authoritative, true)
                .thenApply(metadata -> {
                    if (metadata.partitions >= 1) {
                        topicName = topicName.getPartition(0);
                    }
                    return topicName;
                });
    }

    private CompletableFuture<TopicName> getPublishTopicName(int partition, boolean authoritative) {
        if (topicName.isPartitioned()) {
            return CompletableFuture.completedFuture(topicName);
        }
        return internalGetPartitionedMetadataAsync(authoritative, true)
                .thenApply(metadata -> {
                    if (metadata.partitions < partition) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                String.format("Topic %s has partitions less then %d", topicName.toString(), partition));
                    }
                    topicName = topicName.getPartition(partition - 1);
                    return topicName;
                });
    }

    private String getProducerName(ProducerMessages request) {
        return StringUtils.isNotBlank(request.getProducerName()) ? request.getProducerName() : "RestProducer";
    }

    private CompletableFuture<ProducerAcks> publishMessages(ProducerMessages request, Topic topic, Schema schema,
                                                            SchemaVersion schemaVersion) {
        List<Message> messages = buildMessage(request, schema, topicName);
        List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
        List<ProducerAck> produceMessageResults = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            ProducerAck produceMessageResult = new ProducerAck();
            produceMessageResult.setMessageId(index + "");
            produceMessageResults.add(produceMessageResult);
            CompletableFuture<PositionImpl> publishResult = new CompletableFuture<>();
            topic.publishMessage(messageToByteBuf(messages.get(index)),
                    RestMessagePublishContext.get(publishResult, topic, System.nanoTime()));
            publishResults.add(publishResult);
        }
        return FutureUtil.waitForAll(publishResults)
                .thenApply(__ -> processResults(produceMessageResults, publishResults, schemaVersion));
    }

    private ProducerAcks processResults(List<ProducerAck> produceMessageResults,
                                        List<CompletableFuture<PositionImpl>> publishResults,
                                        SchemaVersion schemaVersion) {
        // process publish message result
        for (int index = 0; index < publishResults.size(); index++) {
            PositionImpl position = publishResults.get(index).join();
            MessageId messageId = new MessageIdImpl(position.getLedgerId(), position.getEntryId(),
                    Integer.parseInt(produceMessageResults.get(index).getMessageId()));
            produceMessageResults.get(index).setMessageId(messageId.toString());
        }
        return new ProducerAcks(produceMessageResults, ((LongSchemaVersion) schemaVersion).getVersion());
    }

    protected CompletableFuture<Void> checkTopicOwnershipAsync(TopicName topicName, boolean authoritative) {
        NamespaceService nsService = pulsar().getNamespaceService();

        LookupOptions options = LookupOptions.builder()
                .authoritative(authoritative)
                .requestHttps(isRequestHttps())
                .readOnly(false)
                .loadTopicsInBundle(false)
                .build();

        return nsService.getWebServiceUrlAsync(topicName, options)
                .thenApply(webUrl -> {
                    if (webUrl == null || !webUrl.isPresent()) {
                        log.info("Unable to get web service url");
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Failed to find ownership for topic:" + topicName);
                    }
                    return webUrl.get();
                }).thenCompose(webUrl -> nsService.isServiceUnitOwnedAsync(topicName)
                        .thenApply(isTopicOwned -> Pair.of(webUrl, isTopicOwned))
                ).thenCompose(pair -> {
                    URL webUrl = pair.getLeft();
                    boolean isTopicOwned = pair.getRight();
                    if (!isTopicOwned) {
                        boolean newAuthoritative = isLeaderBroker(pulsar());
                        int index = uri.getRequestUri().toString().indexOf("/partitions");
                        String partition = "";
                        if (index != -1) {
                            partition = uri.getRequestUri().toString().substring(index);
                        }
                        URI redirect = UriBuilder.fromUri(uri.getRequestUri())
                                .replacePath("/topics/" + topicName.getRestPath() + partition)
                                .host(webUrl.getHost())
                                .port(webUrl.getPort())
                                .replaceQueryParam("authoritative", newAuthoritative)
                                .build();
                        if (log.isDebugEnabled()) {
                            log.debug("Redirecting the rest call to {}", redirect);
                        }
                        return FutureUtil.failedFuture(new WebApplicationException(Response.temporaryRedirect(redirect)
                                .build()));
                    }
                    return pulsar().getBrokerService().getOrCreateTopic(topicName.toString()).thenAccept(__ -> {});
                }).exceptionally(ex -> {
                    Throwable root = FutureUtil.unwrapCompletionException(ex);
                    if (root instanceof IllegalArgumentException || root instanceof IllegalStateException) {
                        if (log.isDebugEnabled()) {
                            log.debug("Failed to find owner for topic: {}", topicName, root);
                        }
                        throw new RestException(Status.PRECONDITION_FAILED, "Can't find owner for topic " + topicName);
                    } else if (root instanceof WebApplicationException) {
                        throw (WebApplicationException) root;
                    } else {
                        throw new RestException(root);
                    }
                });
    }

    // Return error code depends on exception we got indicating if client should retry with same broker.
    protected void extractException(Exception e, ProducerAck produceMessageResult) {
        if (!(e instanceof BrokerServiceException.TopicFencedException && e instanceof ManagedLedgerException)) {
            produceMessageResult.setErrorCode(2);
        } else {
            produceMessageResult.setErrorCode(1);
        }
        produceMessageResult.setErrorMsg(e.getMessage());
    }

    private CompletableFuture<SchemaAndMetadata> getOrAddSchema(
            Topic topic, Optional<SchemaData> schemaData, Optional<LongSchemaVersion> schemaVersion) {
        // If schema version presents try to fetch existing schema.
        String schemaId = TopicName.get(topic.getName()).getSchemaName();
        if (schemaVersion.isPresent()) {
            return pulsar().getSchemaRegistryService().getSchema(schemaId, schemaVersion.get())
                    .thenApply(d -> new SchemaAndMetadata(d.id, d.schema, d.version));

        }
        if (schemaData.isPresent()) {
            return topic.addSchema(schemaData.get())
                    .thenApply(v -> new SchemaAndMetadata(schemaId, schemaData.get(), v));
        }
        return FutureUtil.failedFuture(new SchemaException("empty schema"));
    }

    // Build schemaData from passed in schema string.
    private Optional<SchemaData> getSchemaData(String keySchema, String valueSchema) {
        try {
            SchemaInfoImpl valueSchemaInfo = (valueSchema == null || valueSchema.isEmpty())
                    ? (SchemaInfoImpl) StringSchema.utf8().getSchemaInfo() :
                    ObjectMapperFactory.getThreadLocal()
                            .readValue(valueSchema, SchemaInfoImpl.class);
            if (null == valueSchemaInfo.getName()) {
                valueSchemaInfo.setName(valueSchemaInfo.getType().toString());
            }
            // Value schema only
            if (keySchema == null || keySchema.isEmpty()) {
                return Optional.of(SchemaData.builder()
                        .data(valueSchemaInfo.getSchema())
                        .isDeleted(false)
                        .user("Rest Producer")
                        .timestamp(System.currentTimeMillis())
                        .type(valueSchemaInfo.getType())
                        .props(valueSchemaInfo.getProperties())
                        .build());
            } else {
                // Key_Value schema
                SchemaInfoImpl keySchemaInfo = ObjectMapperFactory.getThreadLocal()
                        .readValue(keySchema, SchemaInfoImpl.class);
                if (null == keySchemaInfo.getName()) {
                    keySchemaInfo.setName(keySchemaInfo.getType().toString());
                }
                SchemaInfo schemaInfo = KeyValueSchemaInfo.encodeKeyValueSchemaInfo("KVSchema-"
                                + topicName.getPartitionedTopicName(),
                        keySchemaInfo, valueSchemaInfo,
                        KeyValueEncodingType.SEPARATED);
                return Optional.of(SchemaData.builder()
                        .data(schemaInfo.getSchema())
                        .isDeleted(false)
                        .user("Rest Producer")
                        .timestamp(System.currentTimeMillis())
                        .type(schemaInfo.getType())
                        .props(schemaInfo.getProperties())
                        .build());
            }
        } catch (IOException e) {
            if (log.isDebugEnabled()) {
                log.debug("Fail to parse schema info for rest produce request with key schema {} and value schema {}"
                        , keySchema, valueSchema);
            }
            return Optional.empty();
        }
    }

    // Convert message to ByteBuf
    public ByteBuf messageToByteBuf(Message message) {
        checkArgument(message instanceof MessageImpl, "Message must be type of MessageImpl.");

        MessageImpl msg = (MessageImpl) message;
        MessageMetadata messageMetadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();
        messageMetadata.setCompression(CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        messageMetadata.setUncompressedSize(payload.readableBytes());

        return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, payload);
    }

    private List<Message> buildMessage(ProducerMessages request, Schema schema, TopicName topicName) {
        return buildMessage(request, schema, topicName, true);
    }

    // Build pulsar message from REST request.
    private List<Message> buildMessage(ProducerMessages request, Schema schema, TopicName topicName,
                                       boolean setProducerName) {
        List<ProducerMessage> messages;
        List<Message> pulsarMessages = new ArrayList<>();

        messages = request.getMessages();
        for (ProducerMessage message : messages) {
            MessageMetadata messageMetadata = new MessageMetadata();
            if (setProducerName) {
                messageMetadata.setProducerName(getProducerName(request));
                messageMetadata.setPublishTime(System.currentTimeMillis());
            }
            messageMetadata.setSequenceId(message.getSequenceId());
            if (null != message.getReplicationClusters()) {
                messageMetadata.addAllReplicateTos(message.getReplicationClusters());
            }

            if (null != message.getProperties()) {
                messageMetadata.addAllProperties(message.getProperties().entrySet().stream().map(entry -> {
                    org.apache.pulsar.common.api.proto.KeyValue keyValue =
                            new org.apache.pulsar.common.api.proto.KeyValue();
                    keyValue.setKey(entry.getKey());
                    keyValue.setValue(entry.getValue());
                    return keyValue;
                }).collect(Collectors.toList()));
            }
            if (null != message.getKey()) {
                // If has key schema, encode partition key, else use plain text.
                if (schema.getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
                    KeyValueSchemaImpl kvSchema = (KeyValueSchemaImpl) schema;
                    messageMetadata.setPartitionKey(
                            Base64.getEncoder().encodeToString(encodeWithSchema(message.getKey(),
                                    kvSchema.getKeySchema())));
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
                KeyValueSchemaImpl kvSchema = (KeyValueSchemaImpl) schema;
                pulsarMessages.add(MessageImpl.create(messageMetadata,
                        ByteBuffer.wrap(encodeWithSchema(message.getPayload(), kvSchema.getValueSchema())),
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
    private byte[] encodeWithSchema(String input, Schema schema) {
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
                          ObjectMapperFactory.getThreadLocal().readTree(input), schema.getSchemaInfo()));
                case AVRO:
                    AvroBaseStructSchema avroSchema = ((AvroBaseStructSchema) schema);
                    Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema.getAvroSchema(), input);
                    DatumReader<GenericData.Record> reader = new GenericDatumReader(avroSchema.getAvroSchema());
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
            if (log.isDebugEnabled()) {
                log.debug("Fail to encode value {} with schema {} for rest produce request", input,
                        new String(schema.getSchemaInfo().getSchema()));
            }
            return new byte[0];
        }
    }

    public CompletableFuture<Void> checkProducePermissionAsync() {
        if (pulsar().getConfiguration().isAuthenticationEnabled()
                && pulsar().getBrokerService().isAuthorizationEnabled()) {
            if (!isClientAuthenticated(clientAppId())) {
                return FutureUtil.failedFuture(new RestException(Status.UNAUTHORIZED,
                        "Need to authenticate to perform the request"));
            }

            return pulsar().getBrokerService().getAuthorizationService()
                    .canProduceAsync(topicName,
                            originalPrincipal() == null ? clientAppId() : originalPrincipal(), clientAuthData())
                    .thenAccept(isAuthorized -> {
                        if (!isAuthorized) {
                            throw new RestException(Status.UNAUTHORIZED,
                                    String.format("Unauthorized to produce to topic %s"
                                            + " with clientAppId [%s] and authdata %s", topicName.toString(),
                                    clientAppId(), clientAuthData()));
                        }
                    });

        }
        return CompletableFuture.completedFuture(null);
    }

    class SchemaAndMetadata {
        public final String id;
        public final SchemaData schema;
        public final SchemaVersion version;

        public SchemaAndMetadata(String id, SchemaData schema, SchemaVersion version) {
            this.id = id;
            this.schema = schema;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SchemaAndMetadata that = (SchemaAndMetadata) o;
            return version == that.version
                    && Objects.equals(id, that.id)
                    && Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, schema, version);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("id", id)
                    .add("schema", schema)
                    .add("version", version)
                    .toString();
        }
    }

}
