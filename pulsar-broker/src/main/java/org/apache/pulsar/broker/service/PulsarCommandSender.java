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
package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.intercept.ResponseHandler;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;

public class PulsarCommandSender {

    private final BrokerInterceptor interceptor;
    private final ChannelHandlerContext ctx;

    public PulsarCommandSender(BrokerInterceptor interceptor, ChannelHandlerContext ctx) {
        this.interceptor = interceptor;
        this.ctx = ctx;
    }

    public void sendPartitionMetadataResponse(PulsarApi.ServerError error, String errorMsg, long requestId,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newPartitionMetadataResponseCommand(error, errorMsg, requestId);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getPartitionMetadataResponse().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendPartitionMetadataResponse(int partitions, long requestId, ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newPartitionMetadataResponseCommand(partitions, requestId);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getPartitionMetadataResponse().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendSuccessResponse(long requestId, ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newSuccessCommand(requestId);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getSuccess().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendErrorResponse(long requestId, PulsarApi.ServerError error, String message,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newErrorCommand(requestId, error, message);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getError().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendProducerSuccessResponse(long requestId, String producerName, SchemaVersion schemaVersion,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, schemaVersion);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getProducerSuccess().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendProducerSuccessResponse(long requestId, String producerName, long lastSequenceId,
            SchemaVersion schemaVersion, ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newProducerSuccessCommand(requestId, producerName, lastSequenceId,
            schemaVersion);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getProducerSuccess().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendSendReceiptResponse(long producerId, long sequenceId, long highestId, long ledgerId,
            long entryId, ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newSendReceiptCommand(producerId, sequenceId, highestId, ledgerId,
                entryId);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getSendReceipt().getMessageId().recycle();
        command.getSendReceipt().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendSendError(long producerId, long sequenceId, PulsarApi.ServerError error, String errorMsg,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newSendErrorCommand(producerId, sequenceId, error, errorMsg);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getSendError().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendGetTopicsOfNamespaceResponse(List<String> topics, long requestId, ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newGetTopicsOfNamespaceResponseCommand(topics, requestId);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetTopicsOfNamespaceResponse().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendGetSchemaResponse(long requestId, SchemaInfo schema, SchemaVersion version,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newGetSchemaResponseCommand(requestId, schema, version);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetSchemaResponse().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendGetSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newGetSchemaResponseErrorCommand(requestId, error, errorMessage);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetSchemaResponse().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendGetOrCreateSchemaResponse(long requestId, SchemaVersion schemaVersion,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newGetOrCreateSchemaResponseCommand(requestId, schemaVersion);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetOrCreateSchemaResponse().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendGetOrCreateSchemaErrorResponse(long requestId, PulsarApi.ServerError error, String errorMessage,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newGetOrCreateSchemaResponseErrorCommand(requestId, error, errorMessage);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getGetOrCreateSchemaResponse().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendConnectedResponse(int clientProtocolVersion, int maxMessageSize, ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newConnectedCommand(clientProtocolVersion, maxMessageSize);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getConnected().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendLookupResponse(String brokerServiceUrl, String brokerServiceUrlTls, boolean authoritative,
            PulsarApi.CommandLookupTopicResponse.LookupType response, long requestId, boolean proxyThroughServiceUrl,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newLookupResponseCommand(brokerServiceUrl, brokerServiceUrlTls,
            authoritative, response, requestId, proxyThroughServiceUrl);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getLookupTopicResponse().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }

    public void sendLookupResponse(PulsarApi.ServerError error, String errorMsg, long requestId,
            ResponseHandler responseHandler) {
        PulsarApi.BaseCommand command = Commands.newLookupErrorResponseCommand(error, errorMsg, requestId);
        responseHandler.complete(command);
        ByteBuf outBuf = Commands.serializeWithSize(command);
        command.getLookupTopicResponse().recycle();
        command.recycle();
        responseHandler.recycle();
        ctx.writeAndFlush(outBuf);
    }
}
