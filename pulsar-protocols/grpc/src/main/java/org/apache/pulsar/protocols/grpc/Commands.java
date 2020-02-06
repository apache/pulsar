package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.protocols.grpc.api.*;

import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;

public class Commands {

    public static SendResult newProducerSuccess(String producerName, long lastSequenceId,
                                                SchemaVersion schemaVersion) {
        CommandProducerSuccess.Builder producerSuccessBuilder = CommandProducerSuccess.newBuilder();
        producerSuccessBuilder.setProducerName(producerName);
        producerSuccessBuilder.setLastSequenceId(lastSequenceId);
        producerSuccessBuilder.setSchemaVersion(ByteString.copyFrom(schemaVersion.bytes()));
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
        ByteString headersAndPayload = ByteString.copyFrom(headersAndPayloadByteBuf.nioBuffer());
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
        ByteString headersAndPayload = ByteString.copyFrom(headersAndPayloadByteBuf.nioBuffer());
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
}
