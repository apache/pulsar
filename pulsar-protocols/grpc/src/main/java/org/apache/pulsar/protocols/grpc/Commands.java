package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.ByteString;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

public class Commands {

    public static PulsarApi.BaseCommand newLookupErrorResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
        PulsarApi.CommandLookupTopicResponse.Builder connectionBuilder = PulsarApi.CommandLookupTopicResponse.newBuilder();
        connectionBuilder.setRequestId(requestId);
        connectionBuilder.setError(error);
        if (errorMsg != null) {
            connectionBuilder.setMessage(errorMsg);
        }
        connectionBuilder.setResponse(PulsarApi.CommandLookupTopicResponse.LookupType.Failed);

        PulsarApi.CommandLookupTopicResponse connectionBroker = connectionBuilder.build();
        return PulsarApi.BaseCommand.newBuilder()
            .setType(PulsarApi.BaseCommand.Type.LOOKUP_RESPONSE)
            .setLookupTopicResponse(connectionBroker)
            .build();
    }

    public static PulsarApi.BaseCommand newPartitionMetadataResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
        PulsarApi.CommandPartitionedTopicMetadataResponse.Builder partitionMetadataResponseBuilder =
            PulsarApi.CommandPartitionedTopicMetadataResponse.newBuilder();
        partitionMetadataResponseBuilder.setRequestId(requestId);
        partitionMetadataResponseBuilder.setError(error);
        partitionMetadataResponseBuilder.setResponse(PulsarApi.CommandPartitionedTopicMetadataResponse.LookupType.Failed);
        if (errorMsg != null) {
            partitionMetadataResponseBuilder.setMessage(errorMsg);
        }

        PulsarApi.CommandPartitionedTopicMetadataResponse partitionMetadataResponse = partitionMetadataResponseBuilder.build();
        return PulsarApi.BaseCommand.newBuilder()
            .setType(PulsarApi.BaseCommand.Type.PARTITIONED_METADATA_RESPONSE)
            .setPartitionMetadataResponse(partitionMetadataResponse)
            .build();
    }

    public static PulsarApi.BaseCommand newError(long requestId, PulsarApi.ServerError error, String message) {
        PulsarApi.CommandError.Builder cmdErrorBuilder = PulsarApi.CommandError.newBuilder();
        cmdErrorBuilder.setRequestId(requestId);
        cmdErrorBuilder.setError(error);
        cmdErrorBuilder.setMessage(message);
        PulsarApi.CommandError cmdError = cmdErrorBuilder.build();
        return PulsarApi.BaseCommand.newBuilder()
            .setType(PulsarApi.BaseCommand.Type.ERROR)
            .setError(cmdError)
            .build();
    }

    public static PulsarApi.BaseCommand newProducerSuccess(long requestId, String producerName, long lastSequenceId,
                                                           SchemaVersion schemaVersion) {
        PulsarApi.CommandProducerSuccess.Builder producerSuccessBuilder = PulsarApi.CommandProducerSuccess.newBuilder();
        producerSuccessBuilder.setRequestId(requestId);
        producerSuccessBuilder.setProducerName(producerName);
        producerSuccessBuilder.setLastSequenceId(lastSequenceId);
        producerSuccessBuilder.setSchemaVersion(ByteString.copyFrom(schemaVersion.bytes()));
        PulsarApi.CommandProducerSuccess producerSuccess = producerSuccessBuilder.build();
        return PulsarApi.BaseCommand.newBuilder()
            .setType(PulsarApi.BaseCommand.Type.PRODUCER_SUCCESS)
            .setProducerSuccess(producerSuccess)
            .build();
    }

    public static PulsarApi.BaseCommand newSendError(long producerId, long sequenceId, PulsarApi.ServerError error, String errorMsg) {
        PulsarApi.CommandSendError.Builder sendErrorBuilder = PulsarApi.CommandSendError.newBuilder();
        sendErrorBuilder.setProducerId(producerId);
        sendErrorBuilder.setSequenceId(sequenceId);
        sendErrorBuilder.setError(error);
        sendErrorBuilder.setMessage(errorMsg);
        PulsarApi.CommandSendError sendError = sendErrorBuilder.build();
        return PulsarApi.BaseCommand.newBuilder()
            .setType(PulsarApi.BaseCommand.Type.SEND_ERROR)
            .setSendError(sendError)
            .build();
    }

    public static PulsarApi.BaseCommand newSendReceipt(long producerId, long sequenceId, long highestId, long ledgerId, long entryId) {
        PulsarApi.CommandSendReceipt.Builder sendReceiptBuilder = PulsarApi.CommandSendReceipt.newBuilder();
        sendReceiptBuilder.setProducerId(producerId);
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
