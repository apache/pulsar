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
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.client.impl.MessageIdImpl;

public class RawMessageImpl implements RawMessage {
    private final MessageIdData id;
    private final ByteBuf headersAndPayload;

    RawMessageImpl(MessageIdData id, ByteBuf headersAndPayload) {
        this.id = id;
        this.headersAndPayload = headersAndPayload.retainedSlice();
    }

    @Override
    public MessageId getMessageId() {
        return new MessageIdImpl(id.getLedgerId(), id.getEntryId(), id.getPartition());
    }

    @Override
    public MessageIdData getMessageIdData() {
        return id;
    }

    @Override
    public ByteBuf getHeadersAndPayload() {
        return headersAndPayload.slice();
    }

    @Override
    public void close() {
        headersAndPayload.release();
    }
}
