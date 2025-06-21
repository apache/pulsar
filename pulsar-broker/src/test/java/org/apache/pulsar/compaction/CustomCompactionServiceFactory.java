/*
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
package org.apache.pulsar.compaction;

import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.PayloadToMessageIdConverter;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.RawMessageImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

public class CustomCompactionServiceFactory extends PulsarCompactionServiceFactory {

    private static final String RETAINED_MESSAGE_INDEXES = "retained.message.indexes";

    @Override
    protected Compactor newCompactor() throws PulsarServerException {
        return new CustomCompactor(getPulsarService());
    }

    public static MessageId convertPayloadToMessageId(PayloadToMessageIdConverter.LastEntry lastEntry) {
        final var buf = Unpooled.wrappedBuffer(lastEntry.getMetadataBuffer());
        try {
            final var metadata = Commands.parseMessageMetadata(buf);
            final int batchSize = metadata.getNumMessagesInBatch();
            final var property = metadata.getPropertiesList().stream()
                    .filter(__ -> __.getKey().equals(RETAINED_MESSAGE_INDEXES)).findAny().orElse(null);
            final int batchIndex;
            if (property == null) {
                batchIndex = batchSize - 1;
            } else {
                final var indexes = Arrays.stream(property.getValue().split(",")).map(Integer::valueOf).toList();
                batchIndex = indexes.get(indexes.size() - 1);
            }
            return new BatchMessageIdImpl(lastEntry.getLedgerId(), lastEntry.getEntryId(),
                    lastEntry.getPartitionIndex(), batchIndex);
        } finally {
            buf.release();
        }
    }

    public static class PayloadProcessor implements MessagePayloadProcessor {

        @Override
        public <T> void process(MessagePayload payload, MessagePayloadContext context, Schema<T> schema,
                                Consumer<Message<T>> messageConsumer) throws Exception {
            final var property = context.getProperty(RETAINED_MESSAGE_INDEXES);
            if (property == null) {
                MessagePayloadProcessor.DEFAULT.process(payload, context, schema, messageConsumer);
                return;
            }
            final var numMessages = context.getNumMessages();
            final var indexes = Arrays.stream(property.split(",")).map(Integer::valueOf).collect(Collectors.toSet());
            for (int i = 0; i < numMessages; i++) {
                final var msg = context.getMessageAt(i, numMessages, payload, true, schema);
                if (indexes.contains(i)) {
                    messageConsumer.accept(msg);
                }
            }
        }
    }

    private static class CustomCompactor extends PublishingOrderCompactor {

        public CustomCompactor(PulsarService pulsarService) throws PulsarServerException {
            super(pulsarService.getConfiguration(), pulsarService.getClient(), pulsarService.getBookKeeperClient(),
                    pulsarService.getCompactorExecutor());
        }

        // This is a simple implementation that assumes all messages are not compressed and have partition keys
        @Override
        protected Optional<RawMessage> rebatchMessage(String topic, RawMessage msg, MessageMetadata metadata,
                                                      BiPredicate<String, MessageId> filter, boolean retainNullKey)
                throws IOException {
            final var payload = msg.getHeadersAndPayload();
            if (metadata == null) {
                metadata = Commands.parseMessageMetadata(payload);
            } else {
                Commands.skipMessageMetadata(payload);
            }

            final var batchSize = metadata.getNumMessagesInBatch();
            final var singleMessageMetadata = new SingleMessageMetadata();
            final var retainedMessageIndexes = new ArrayList<String>();
            final var batchBuffer = PulsarByteBufAllocator.DEFAULT.buffer(payload.capacity());
            // The difference from the built-in compactor's behavior is that the compactedOut field is no longer set.
            // Instead, the retained messages' indexes are serialized into a property.
            for (int i = 0; i < batchSize; i++) {
                final var singleMessagePayload = Commands.deSerializeSingleMessageInBatch(payload,
                        singleMessageMetadata, 0, batchSize);
                final var id = new BatchMessageIdImpl(msg.getMessageIdData().getLedgerId(),
                        msg.getMessageIdData().getEntryId(), msg.getMessageIdData().getPartition(), i);
                if (singleMessageMetadata.isCompactedOut()) {
                    retainedMessageIndexes.add(Integer.toString(i));
                    Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadata, Unpooled.EMPTY_BUFFER,
                            batchBuffer);
                } else if (filter.test(singleMessageMetadata.getPartitionKey(), id)
                        && singleMessagePayload.readableBytes() > 0) {
                    retainedMessageIndexes.add(Integer.toString(i));
                    Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadata,
                            singleMessagePayload, batchBuffer);
                } else {
                    Commands.serializeSingleMessageInBatchWithPayload(singleMessageMetadata, Unpooled.EMPTY_BUFFER,
                            batchBuffer);
                }
                singleMessagePayload.release();
            }
            if (retainedMessageIndexes.isEmpty()) {
                return Optional.empty();
            }
            metadata.addProperty().setKey(RETAINED_MESSAGE_INDEXES)
                    .setValue(String.join(",", retainedMessageIndexes));
            final var metadataAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                    metadata, batchBuffer);
            try {
                return Optional.of(new RawMessageImpl(msg.getMessageIdData(), metadataAndPayload));
            } finally {
                metadataAndPayload.release();
            }
        }
    }
}
