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
import io.netty.util.Recycler;
import io.netty.util.ReferenceCountUtil;
import lombok.Getter;
import lombok.NonNull;
import org.apache.pulsar.client.api.MessagePayload;

/**
 * A wrapper of {@link ByteBuf} that implements {@link MessagePayload}.
 */
public class MessagePayloadImpl implements MessagePayload {

    private static final Recycler<MessagePayloadImpl> RECYCLER = new Recycler<MessagePayloadImpl>() {
        @Override
        protected MessagePayloadImpl newObject(Handle<MessagePayloadImpl> handle) {
            return new MessagePayloadImpl(handle);
        }
    };

    private final Recycler.Handle<MessagePayloadImpl> recyclerHandle;
    @Getter
    private ByteBuf byteBuf;

    public static MessagePayloadImpl create(@NonNull final ByteBuf byteBuf) {
        final MessagePayloadImpl payload = RECYCLER.get();
        payload.byteBuf = byteBuf;
        return payload;
    }

    private MessagePayloadImpl(final Recycler.Handle<MessagePayloadImpl> handle) {
        this.recyclerHandle = handle;
    }

    @Override
    public void release() {
        ReferenceCountUtil.release(byteBuf);
        byteBuf = null;
        recyclerHandle.recycle(this);
    }

    @Override
    public byte[] copiedBuffer() {
        final int readable = byteBuf.readableBytes();
        if (readable > 0) {
            final byte[] bytes = new byte[readable];
            byteBuf.getBytes(byteBuf.readerIndex(), bytes);
            return bytes;
        } else {
            return new byte[0];
        }
    }
}
