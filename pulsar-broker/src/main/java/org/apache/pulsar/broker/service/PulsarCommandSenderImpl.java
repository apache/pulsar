package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;

@Slf4j
public class PulsarCommandSenderImpl implements PulsarCommandSender {

    private final BrokerInterceptor interceptor;
    private final ServerCnx cnx;

    public PulsarCommandSenderImpl(BrokerInterceptor interceptor, ServerCnx cnx) {
        this.interceptor = interceptor;
        this.cnx = cnx;
    }

    @Override
    public void sendPartitionMetadataResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
        PulsarApi.BaseCommand command = Commands.newPartitionMetadataResponseCommand(error, errorMsg, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getPartitionMetadataResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendPartitionMetadataResponse(int partitions, long requestId) {
        PulsarApi.BaseCommand command = Commands.newPartitionMetadataResponseCommand(partitions, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getPartitionMetadataResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendSuccessResponse(long requestId) {
        PulsarApi.BaseCommand command = Commands.newSuccessCommand(requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getSuccess().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendErrorResponse(long requestId, PulsarApi.ServerError error, String message) {
        PulsarApi.BaseCommand command = Commands.newErrorCommand(requestId, error, message);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getError().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, SchemaVersion schemaVersion) {
        PulsarApi.BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getProducerSuccess().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendProducerSuccessResponse(long requestId, String producerName, long lastSequenceId,
                                            SchemaVersion schemaVersion) {
        PulsarApi.BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, lastSequenceId,
                schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getProducerSuccess().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendSendReceiptResponse(long producerId, long sequenceId, long highestId, long ledgerId,
                                        long entryId) {
        PulsarApi.BaseCommand command = Commands.newSendReceiptCommand(producerId, sequenceId, highestId, ledgerId,
                entryId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getSendReceipt().getMessageId().recycle();
        command.getSendReceipt().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendSendError(long producerId, long sequenceId, PulsarApi.ServerError error, String errorMsg) {
        PulsarApi.BaseCommand command = Commands.newSendErrorCommand(producerId, sequenceId, error, errorMsg);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getSendError().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetTopicsOfNamespaceResponse(List<String> topics, long requestId) {
        PulsarApi.BaseCommand command = Commands.newGetTopicsOfNamespaceResponseCommand(topics, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetTopicsOfNamespaceResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version) {
        PulsarApi.BaseCommand command = Commands.newGetSchemaResponseCommand(requestId, schema, version);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetSchemaResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage) {
        PulsarApi.BaseCommand command = Commands.newGetSchemaResponseErrorCommand(requestId, error, errorMessage);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetSchemaResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion) {
        PulsarApi.BaseCommand command = Commands.newGetOrCreateSchemaResponseCommand(requestId, schemaVersion);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetOrCreateSchemaResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendGetOrCreateSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage) {
        PulsarApi.BaseCommand command = Commands.newGetOrCreateSchemaResponseErrorCommand(requestId, error, errorMessage);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetOrCreateSchemaResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendConnectedResponse(int clientProtocolVersion, int maxMessageSize) {
        PulsarApi.BaseCommand command = Commands.newConnectedCommand(clientProtocolVersion, maxMessageSize);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getConnected().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
                                   PulsarApi.CommandLookupTopicResponse.LookupType response, long requestId, boolean proxyThroughServiceUrl) {
        PulsarApi.BaseCommand command = Commands.newLookupResponseCommand(brokerServiceUrl, brokerServiceUrlTls,
                authoritative, response, requestId, proxyThroughServiceUrl);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getLookupTopicResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    @Override
    public void sendLookupResponse(PulsarApi.ServerError error, String errorMsg, long requestId) {
        PulsarApi.BaseCommand command = Commands.newLookupErrorResponseCommand(error, errorMsg, requestId);
        safeIntercept(command, cnx);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getLookupTopicResponse().recycle();
        command.recycle();
        cnx.ctx().writeAndFlush(outBuf);
    }

    private void safeIntercept(PulsarApi.BaseCommand command, ServerCnx cnx) {
        try {
            this.interceptor.onPulsarCommand(command, cnx);
        } catch (Exception e) {
            log.error("Failed to execute command {} on broker interceptor.", command.getType(), e);
        }
    }
}
