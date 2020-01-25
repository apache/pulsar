package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.ByteString;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

public class Commands {

    public static PulsarApi.BaseCommand newProducerSuccess(String producerName, long lastSequenceId,
                                                           SchemaVersion schemaVersion) {
        PulsarApi.CommandProducerSuccess.Builder producerSuccessBuilder = PulsarApi.CommandProducerSuccess.newBuilder();
        producerSuccessBuilder.setProducerName(producerName);
        producerSuccessBuilder.setLastSequenceId(lastSequenceId);
        producerSuccessBuilder.setSchemaVersion(ByteString.copyFrom(schemaVersion.bytes()));
        PulsarApi.CommandProducerSuccess producerSuccess = producerSuccessBuilder.build();
        return PulsarApi.BaseCommand.newBuilder()
            .setType(PulsarApi.BaseCommand.Type.PRODUCER_SUCCESS)
            .setProducerSuccess(producerSuccess)
            .build();
    }

    public static PulsarApi.BaseCommand newSendError(long sequenceId, PulsarApi.ServerError error, String errorMsg) {
        PulsarApi.CommandSendError.Builder sendErrorBuilder = PulsarApi.CommandSendError.newBuilder();
        sendErrorBuilder.setSequenceId(sequenceId);
        sendErrorBuilder.setError(error);
        sendErrorBuilder.setMessage(errorMsg);
        PulsarApi.CommandSendError sendError = sendErrorBuilder.build();
        return PulsarApi.BaseCommand.newBuilder()
            .setType(PulsarApi.BaseCommand.Type.SEND_ERROR)
            .setSendError(sendError)
            .build();
    }

    public static PulsarApi.BaseCommand newSendReceipt(long sequenceId, long highestId, long ledgerId, long entryId) {
        PulsarApi.CommandSendReceipt.Builder sendReceiptBuilder = PulsarApi.CommandSendReceipt.newBuilder();
        sendReceiptBuilder.setSequenceId(sequenceId);
        sendReceiptBuilder.setHighestSequenceId(highestId);
        PulsarApi.MessageIdData.Builder messageIdBuilder = PulsarApi.MessageIdData.newBuilder();
        messageIdBuilder.setLedgerId(ledgerId);
        messageIdBuilder.setEntryId(entryId);
        PulsarApi.MessageIdData messageId = messageIdBuilder.build();
        sendReceiptBuilder.setMessageId(messageId);
        PulsarApi.CommandSendReceipt sendReceipt = sendReceiptBuilder.build();
        return PulsarApi.BaseCommand.newBuilder()
            .setType(PulsarApi.BaseCommand.Type.SEND_RECEIPT)
            .setSendReceipt(sendReceipt)
            .build();
    }




}
