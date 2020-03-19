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
package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.protocols.grpc.api.*;

import java.util.Map;

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

    public static CommandAuthChallenge newAuthChallenge(String authMethod, org.apache.pulsar.common.api.AuthData brokerData, long stateId) {
        CommandAuthChallenge.Builder challengeBuilder = CommandAuthChallenge.newBuilder();
        challengeBuilder.setChallenge(AuthData.newBuilder()
                .setAuthData(ByteString.copyFrom(brokerData.getBytes()))
                .setAuthMethodName(authMethod)
                .setAuthStateId(stateId)
                .build());
        return challengeBuilder.build();
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

        if (metadata != null) {
            producerBuilder.putAllMetadata(metadata);
        }

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

    public static CommandGetSchema newGetSchema(String topic, SchemaVersion version) {
        CommandGetSchema.Builder schema = CommandGetSchema.newBuilder();
        schema.setTopic(topic);
        if (version != null) {
            schema.setSchemaVersion(ByteString.copyFrom(version.bytes()));
        }

        return schema.build();
    }

    public static CommandGetSchemaResponse newGetSchemaResponse(SchemaInfo schema, SchemaVersion version) {
        CommandGetSchemaResponse.Builder schemaResponse = CommandGetSchemaResponse.newBuilder()
                .setSchemaVersion(ByteString.copyFrom(version.bytes()))
                .setSchema(getSchema(schema));

        return schemaResponse.build();
    }

    public static CommandLookupTopicResponse newLookupResponse(String grpcServiceHost, Integer grpcServicePort,
            Integer grpcServicePortTls, boolean authoritative, CommandLookupTopicResponse.LookupType response,
            boolean proxyThroughServiceUrl) {
        CommandLookupTopicResponse.Builder commandLookupTopicResponseBuilder = CommandLookupTopicResponse.newBuilder();
        commandLookupTopicResponseBuilder.setGrpcServiceHost(grpcServiceHost);
        if (grpcServicePort != null) {
            commandLookupTopicResponseBuilder.setGrpcServicePort(grpcServicePort);
        }
        if (grpcServicePortTls != null) {
            commandLookupTopicResponseBuilder.setGrpcServicePortTls(grpcServicePortTls);
        }
        commandLookupTopicResponseBuilder.setResponse(response);
        commandLookupTopicResponseBuilder.setAuthoritative(authoritative);
        commandLookupTopicResponseBuilder.setProxyThroughServiceUrl(proxyThroughServiceUrl);

        return commandLookupTopicResponseBuilder.build();
    }
}
