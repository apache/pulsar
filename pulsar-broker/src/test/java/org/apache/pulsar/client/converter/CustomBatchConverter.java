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
package org.apache.pulsar.client.converter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import org.apache.pulsar.client.api.EntryContext;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.PayloadConverter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.DefaultPayloadConverter;
import org.apache.pulsar.client.impl.MessagePayloadImpl;
import org.apache.pulsar.client.impl.MessagePayloadUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CustomBatchConverter implements PayloadConverter {

    private static final PayloadConverter DEFAULT = new DefaultPayloadConverter();

    @Override
    public <T> Iterable<Message<T>> convert(EntryContext context, MessagePayload payload, Schema<T> schema) {
        final String value = context.getProperty(CustomBatchFormat.KEY);
        if (value == null || !value.equals(CustomBatchFormat.VALUE)) {
            return DEFAULT.convert(context, payload, schema);
        }

        final ByteBuf buf = MessagePayloadUtils.convertToByteBuf(payload);
        final CustomBatchFormat.StringIterable strings = CustomBatchFormat.deserialize(buf);
        final Iterator<String> stringIterator = strings.iterator();
        final int numMessages = context.getNumMessages();
        final List<ByteBuf> bufList = new ArrayList<>();

        return () -> new Iterator<Message<T>>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                final boolean result = stringIterator.hasNext();
                if (!result) {
                    bufList.forEach(ReferenceCounted::release);
                    buf.release();
                }
                return result;
            }

            @Override
            public Message<T> next() {
                final String value = stringIterator.next();
                final ByteBuf valueBuf = Unpooled.wrappedBuffer(Schema.STRING.encode(value));
                bufList.add(valueBuf);
                return context.newSingleMessage(
                        index++, numMessages, MessagePayloadImpl.create(valueBuf), false, schema);
            }
        };
    }
}
