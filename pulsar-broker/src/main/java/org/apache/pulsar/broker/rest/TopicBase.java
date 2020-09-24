package org.apache.pulsar.broker.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DateSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.InstantSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.LocalDateSchema;
import org.apache.pulsar.client.impl.schema.LocalDateTimeSchema;
import org.apache.pulsar.client.impl.schema.LocalTimeSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.TimeSchema;
import org.apache.pulsar.client.impl.schema.TimestampSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ProduceMessageRequest;
import org.apache.pulsar.common.policies.data.ProduceMessageResponse;
import org.apache.pulsar.common.policies.data.RestProduceMessage;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class TopicBase extends PersistentTopicsBase {

    private ConcurrentOpenHashMap<String, List<Integer>> owningTopics = new ConcurrentOpenHashMap<>();

    protected  void internalPublishMessage(AsyncResponse asyncResponse, ProduceMessageRequest request,
                                           boolean authoritative, String producerName) {
        String topic = topicName.getPartitionedTopicName();
        if (owningTopics.containsKey(topic)) {
            publishMessagesToMultiplePartitions(topicName, request, producerName, owningTopics.get(topicName.getPartitionedTopicName()), asyncResponse);
        } else {
            // look up topic first
            List<String> brokerForRedirect = findOwnerBrokerForTopic(authoritative);
            // Current broker doesn't own the topic or any partition of the topic, redirect client to a broker
            // that own partition of the topic or know who own partition of the topic.
            if (!owningTopics.containsKey(topicName.getPartitionedTopicName())) {
                if (brokerForRedirect.isEmpty()) {
                    // No broker to redirect, means look up for some partitions failed, client should retry with other brokers.
                    asyncResponse.resume(new RestException(Status.NOT_FOUND, "Can't find owner of given topic."));
                } else {
                    // Redirect client to other broker owns the topic or know which broker own the topic.
                    try {
                        URI redirectURI = new URI(String.format("%s%s", brokerForRedirect.get(0), uri.getPath()));
                        asyncResponse.resume(Response.temporaryRedirect(redirectURI));
                    } catch (URISyntaxException | NullPointerException e) {
                        log.error("Error in preparing redirect url with rest produce message request for topic  {}: {}",
                                topicName, e.getMessage(), e);
                        asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, "Fail to redirect client request."));
                    }
                }
            }
            publishMessagesToMultiplePartitions(topicName, request, producerName, owningTopics.get(topicName.getPartitionedTopicName()), asyncResponse);
        }
    }

    protected void internalPublishMessageToPartition(String topic) {

    }

    private CompletableFuture<PositionImpl> publishMessageToSinglePartition(String topic, Message message, String producerName) {
        CompletableFuture<PositionImpl> publishResult = new CompletableFuture<>();
        pulsar().getBrokerService().getTopic(topic, false).thenAccept(t -> {
            if (!t.isPresent()) {
                publishResult.completeExceptionally(new BrokerServiceException.TopicNotFoundException(""));
            } else {
                t.get().publishMessage(messageToByteBuf(message, producerName),
                        RestMessagePublishContext.get(publishResult, t.get(), System.nanoTime()));
            }
        });

        return publishResult;
    }

    private void publishMessagesToMultiplePartitions(TopicName topicName, ProduceMessageRequest request,
                                                     String producerName, List<Integer> partitionIndexes,
                                                     AsyncResponse asyncResponse) {
        try {
            List<Message> messages = buildMessage(request);
            List<CompletableFuture<PositionImpl>> publishResults = new ArrayList<>();
            List<ProduceMessageResponse.ProduceMessageResult> produceMessageResults = new ArrayList<>();
            for (int index = 0; index < messages.size(); index++) {
                ProduceMessageResponse.ProduceMessageResult produceMessageResult = new ProduceMessageResponse.ProduceMessageResult();
                produceMessageResult.setPartition(partitionIndexes.get(index % partitionIndexes.size()));
                produceMessageResults.add(produceMessageResult);
                publishResults.add(publishMessageToSinglePartition(topicName.getPartition(partitionIndexes.get(index % partitionIndexes.size())).getLocalName(),
                    messages.get(index), producerName));
            }
            FutureUtil.waitForAll(publishResults);
            asyncResponse.resume(new ProduceMessageResponse(produceMessageResults));
        } catch (JsonProcessingException e) {
            asyncResponse.resume(new RestException(Status.BAD_REQUEST, ""));
        }
    }

    // Look up topic owner for given topic.
    private List<String> findOwnerBrokerForTopic(boolean authoritative) {
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

        return redirectAddresses;
    }

    // Look up topic owner for non-partitioned topic or single topic partition.
    private void lookUpBrokerForTopic(TopicName partitionedTopicName, boolean authoritative, List<String> redirectAddresses) {
        CompletableFuture<Optional<LookupResult>> lookupFuture = pulsar().getNamespaceService()
                .getBrokerServiceUrlAsync(partitionedTopicName, LookupOptions.builder().authoritative(authoritative).loadTopicsInBundle(false).build());

        lookupFuture.thenAccept(optionalResult -> {
            if (optionalResult == null || !optionalResult.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug("Fail to lookup topic for rest produce message request for topic {}, current broker is owner broker: {}",
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
                        (key) -> Collections.synchronizedList(new LinkedList<>())).add(partitionedTopicName.getPartitionIndex());
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

    protected void internalPublishMessageToPartition(AsyncResponse asyncResponse, ProduceMessageRequest produceMessageRequest,
                                          boolean authoritative, UriInfo uriInfo) {

    }

    private Schema getSchema(String keySchemaJson, String valueSchemaJson) {
        Schema keySchema = getSchemaFromJson(keySchemaJson);
        Schema valueSchema = getSchemaFromJson(valueSchemaJson);
        return KeyValueSchema.of(keySchema, valueSchema);
    }

    private Schema getSchemaFromJson(String schema) {
        SchemaInfo schemaInfo;
        try {
            schemaInfo = ObjectMapperFactory.getThreadLocal().readValue(schema, SchemaInfo.class);
            switch (schemaInfo.getType()) {
                case INT8:
                    return ByteSchema.of();
                case INT16:
                    return ShortSchema.of();
                case INT32:
                    return IntSchema.of();
                case INT64:
                    return LongSchema.of();
                case STRING:
                    return StringSchema.utf8();
                case FLOAT:
                    return FloatSchema.of();
                case DOUBLE:
                    return DoubleSchema.of();
                case BOOLEAN:
                    return BooleanSchema.of();
                case BYTES:
                    return BytesSchema.of();
                case DATE:
                    return DateSchema.of();
                case TIME:
                    return TimeSchema.of();
                case TIMESTAMP:
                    return TimestampSchema.of();
                case INSTANT:
                    return InstantSchema.of();
                case LOCAL_DATE:
                    return LocalDateSchema.of();
                case LOCAL_TIME:
                    return LocalTimeSchema.of();
                case LOCAL_DATE_TIME:
                    return LocalDateTimeSchema.of();
                case JSON:
                case AVRO:
                    return GenericSchemaImpl.of(schemaInfo);
                case KEY_VALUE:
                default:
                    throw new IllegalArgumentException("Schema type of '"  + schemaInfo.getType() +
                            "' is not supported yet");
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new AutoProduceBytesSchema();
    }

    // Convert message to ByteBuf
    public ByteBuf messageToByteBuf(Message message, String producerName) {
        checkArgument(message instanceof MessageImpl, "");

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
    private List<Message> buildMessage(ProduceMessageRequest produceMessageRequest) throws JsonProcessingException {
        List<RestProduceMessage> messages;
        List<Message> pulsarMessages = new ArrayList<>();
        Schema schema = getSchema(produceMessageRequest.getKeySchema(), produceMessageRequest.getValueSchema());

        try {
            messages = ObjectMapperFactory.getThreadLocal().readValue(produceMessageRequest.getMessages(), new TypeReference<List<RestProduceMessage>>(){});
            for (RestProduceMessage message : messages) {
                PulsarApi.MessageMetadata.Builder metadataBuilder = PulsarApi.MessageMetadata.newBuilder();
                metadataBuilder.addAllReplicateTo(message.getReplicationClusters());
                metadataBuilder.setPartitionKey(message.getKey());
                metadataBuilder.setEventTime(message.getEventTime());
                metadataBuilder.setSequenceId(message.getSequenceId());
                metadataBuilder.setDeliverAtTime(message.getDeliverAt());
                pulsarMessages.add(MessageImpl.create(metadataBuilder, ByteBuffer.wrap(message.getValue().getBytes(UTF_8)), schema));
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
