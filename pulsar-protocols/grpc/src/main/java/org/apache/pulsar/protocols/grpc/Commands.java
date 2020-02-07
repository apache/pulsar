package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.protocols.grpc.api.*;

import java.util.Map;
import java.util.stream.Collectors;

import static com.google.protobuf.ByteString.copyFrom;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.apache.pulsar.protocols.grpc.Constants.ERROR_CODE_METADATA_KEY;

public class Commands {

    public static StatusRuntimeException newStatusException(Status status, String message, Throwable exception, ServerError code) {
        Metadata metadata  = new Metadata();
        metadata.put(ERROR_CODE_METADATA_KEY, String.valueOf(code.getNumber()));
        return status.withDescription(message)
            .withCause(exception)
            .asRuntimeException(metadata);
    }

    public static StatusRuntimeException newStatusException(Status status, Throwable exception, ServerError code) {
        return newStatusException(status, exception.getMessage(), exception, code);
    }

    public static SendResult newProducerSuccess(String producerName, long lastSequenceId,
                                                SchemaVersion schemaVersion) {
        CommandProducerSuccess.Builder producerSuccessBuilder = CommandProducerSuccess.newBuilder();
        producerSuccessBuilder.setProducerName(producerName);
        producerSuccessBuilder.setLastSequenceId(lastSequenceId);
        producerSuccessBuilder.setSchemaVersion(copyFrom(schemaVersion.bytes()));
        CommandProducerSuccess producerSuccess = producerSuccessBuilder.build();
        return SendResult.newBuilder()
            .setProducerSuccess(producerSuccess)
            .build();
    }

    public static CommandSend newSend(long sequenceId, int numMessages, ChecksumType checksumType,
                                      MessageMetadata messageMetadata, ByteBuf payload) {
        return newSend(sequenceId, numMessages, 0, 0, checksumType, messageMetadata, payload);
    }

    public static CommandSend newSend(long lowestSequenceId, long highestSequenceId, int numMessages,
                                      ChecksumType checksumType, MessageMetadata messageMetadata, ByteBuf payload) {
        return newSend(lowestSequenceId, highestSequenceId, numMessages, 0, 0,
            checksumType, messageMetadata, payload);
    }

    public static CommandSend newSend(long sequenceId, int numMessages,
                                      long txnIdLeastBits, long txnIdMostBits, ChecksumType checksumType,
                                      MessageMetadata messageData, ByteBuf payload) {
        CommandSend.Builder sendBuilder = CommandSend.newBuilder();
        sendBuilder.setSequenceId(sequenceId);
        if (numMessages > 1) {
            sendBuilder.setNumMessages(numMessages);
        }
        if (txnIdLeastBits > 0) {
            sendBuilder.setTxnidLeastBits(txnIdLeastBits);
        }
        if (txnIdMostBits > 0) {
            sendBuilder.setTxnidMostBits(txnIdMostBits);
        }
        ByteBuf headersAndPayloadByteBuf = serializeMetadataAndPayload(checksumType, messageData, payload);
        ByteString headersAndPayload = copyFrom(headersAndPayloadByteBuf.nioBuffer());
        sendBuilder.setHeadersAndPayload(headersAndPayload);

        return sendBuilder.build();
    }

    public static CommandSend newSend(long lowestSequenceId, long highestSequenceId, int numMessages,
                                      long txnIdLeastBits, long txnIdMostBits, ChecksumType checksumType,
                                      MessageMetadata messageData, ByteBuf payload) {
        CommandSend.Builder sendBuilder = CommandSend.newBuilder();
        sendBuilder.setSequenceId(lowestSequenceId);
        sendBuilder.setHighestSequenceId(highestSequenceId);
        if (numMessages > 1) {
            sendBuilder.setNumMessages(numMessages);
        }
        if (txnIdLeastBits > 0) {
            sendBuilder.setTxnidLeastBits(txnIdLeastBits);
        }
        if (txnIdMostBits > 0) {
            sendBuilder.setTxnidMostBits(txnIdMostBits);
        }
        ByteBuf headersAndPayloadByteBuf = serializeMetadataAndPayload(checksumType, messageData, payload);
        ByteString headersAndPayload = copyFrom(headersAndPayloadByteBuf.nioBuffer());
        sendBuilder.setHeadersAndPayload(headersAndPayload);

        return sendBuilder.build();
    }

    public static SendResult newSendError(long sequenceId, ServerError error, String errorMsg) {
        CommandSendError.Builder sendErrorBuilder = CommandSendError.newBuilder();
        sendErrorBuilder.setSequenceId(sequenceId);
        sendErrorBuilder.setError(error);
        sendErrorBuilder.setMessage(errorMsg);
        CommandSendError sendError = sendErrorBuilder.build();
        return SendResult.newBuilder()
            .setSendError(sendError)
            .build();
    }

    public static SendResult newSendReceipt(long sequenceId, long highestId, long ledgerId, long entryId) {
        CommandSendReceipt.Builder sendReceiptBuilder = CommandSendReceipt.newBuilder();
        sendReceiptBuilder.setSequenceId(sequenceId);
        sendReceiptBuilder.setHighestSequenceId(highestId);
        MessageIdData.Builder messageIdBuilder = MessageIdData.newBuilder();
        messageIdBuilder.setLedgerId(ledgerId);
        messageIdBuilder.setEntryId(entryId);
        MessageIdData messageId = messageIdBuilder.build();
        sendReceiptBuilder.setMessageId(messageId);
        CommandSendReceipt sendReceipt = sendReceiptBuilder.build();
        return SendResult.newBuilder()
            .setSendReceipt(sendReceipt)
            .build();
    }

    public static CommandProducer newProducer(String topic, String producerName,
                                      boolean encrypted, Map<String, String> metadata, SchemaInfo schemaInfo,
                                      long epoch, boolean userProvidedProducerName) {
        CommandProducer.Builder producerBuilder = CommandProducer.newBuilder();
        producerBuilder.setTopic(topic);
        producerBuilder.setEpoch(epoch);
        if (producerName != null) {
            producerBuilder.setProducerName(producerName);
        }
        producerBuilder.setUserProvidedProducerName(userProvidedProducerName);
        producerBuilder.setEncrypted(encrypted);

        producerBuilder.putAllMetadata(metadata);

        if (null != schemaInfo) {
            producerBuilder.setSchema(getSchema(schemaInfo));
        }

        return producerBuilder.build();
    }

    public static CommandProducer newProducer(String topic, String producerName,
                                      Map<String, String> metadata) {
        return newProducer(topic, producerName, false, metadata);
    }

    public static CommandProducer newProducer(String topic, String producerName,
                                      boolean encrypted, Map<String, String> metadata) {
        return newProducer(topic, producerName, encrypted, metadata, null, 0, false);
    }

    private static Schema getSchema(SchemaInfo schemaInfo) {
        Schema.Builder builder = Schema.newBuilder()
            .setName(schemaInfo.getName())
            .setSchemaData(copyFrom(schemaInfo.getSchema()))
            .setType(getSchemaType(schemaInfo.getType()))
            .putAllProperties(schemaInfo.getProperties());
        return builder.build();
    }

    public static Schema.Type getSchemaType(SchemaType type) {
        if (type.getValue() < 0) {
            return Schema.Type.None;
        } else {
            return Schema.Type.forNumber(type.getValue());
        }
    }

    public static SchemaType getSchemaType(Schema.Type type) {
        if (type.getNumber() < 0) {
            // this is unexpected
            return SchemaType.NONE;
        } else {
            return SchemaType.valueOf(type.getNumber());
        }
    }

}
