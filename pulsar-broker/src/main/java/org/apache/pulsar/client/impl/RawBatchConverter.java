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
package org.apache.pulsar.client.impl;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pulsar.client.api.RawMessage;

import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawBatchConverter {
    private static final Logger log = LoggerFactory.getLogger(RawBatchConverter.class);

    private static MessageMetadata mergeMetadata(MessageMetadata batchMetadata,
                                                 SingleMessageMetadata.Builder singleMessageMetadata) {
        // is uncompressed size correct?
        return batchMetadata.toBuilder()
            .setNumMessagesInBatch(1)
            .setUncompressedSize(singleMessageMetadata.getPayloadSize())
            .addAllProperties(singleMessageMetadata.getPropertiesList())
            .setPartitionKey(singleMessageMetadata.getPartitionKey()).build();
    }

    public static boolean isBatch(RawMessage msg) {
        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        int batchSize = metadata.getNumMessagesInBatch();
        return batchSize > 0;
    }

    public static Collection<RawMessage> explodeBatch(RawMessage msg) throws IOException {
        assert(msg.getMessageIdData().getBatchIndex() == -1);

        ByteBuf payload = msg.getHeadersAndPayload();
        MessageMetadata metadata = Commands.parseMessageMetadata(payload);
        int batchSize = metadata.getNumMessagesInBatch();
        List<RawMessage> exploded = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            SingleMessageMetadata.Builder singleMessageMetadataBuilder = SingleMessageMetadata.newBuilder();
            ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(payload,
                                                                                    singleMessageMetadataBuilder,
                                                                                    0, batchSize);

            // serializeMetadataAndPayload takes ownership of the the payload
            ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(
                    Commands.ChecksumType.Crc32c, mergeMetadata(metadata, singleMessageMetadataBuilder),
                    singleMessagePayload);
            exploded.add(new RawMessageImpl(msg.getMessageIdData().toBuilder().setBatchIndex(i).build(),
                                            metadataAndPayload));
            metadataAndPayload.release();
            singleMessagePayload.release();
        }
        return exploded;
    }
}
