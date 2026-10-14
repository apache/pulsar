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
package org.apache.pulsar.client.processor;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import lombok.CustomLog;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

@RequiredArgsConstructor
@CustomLog
public class CustomBatchProducer {

    private final List<String> messages = new ArrayList<>();
    private final PersistentTopic persistentTopic;
    private final int batchingMaxMessages;

    public void sendAsync(final String value) {
        messages.add(value);
        if (messages.size() >= batchingMaxMessages) {
            flush();
        }
    }

    public void flush() {
        final ByteBuf buf = CustomBatchFormat.serialize(messages);
        final ByteBuf headerAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.None,
                createCustomMetadata(), buf);
        buf.release();
        persistentTopic.publishMessage(headerAndPayload, (e, ledgerId, entryId) -> {
            if (e == null) {
                log.info()
                        .attr("successfully", persistentTopic.getName())
                        .attr("ledgerId", ledgerId)
                        .attr("entryId", entryId)
                        .log("Send successfully to");
            } else {
                log.error().exceptionMessage(e).log("Failed to send");
            }
        });
        messages.clear();
    }

    private static MessageMetadata createCustomMetadata() {
        final MessageMetadata messageMetadata = new MessageMetadata();
        // Here are required fields
        messageMetadata.setProducerName("");
        messageMetadata.setSequenceId(0L);
        messageMetadata.setPublishTime(0L);
        // Add the property to identify the message format
        messageMetadata.addProperty().setKey(CustomBatchFormat.KEY).setValue(CustomBatchFormat.VALUE);
        return messageMetadata;
    }
}
