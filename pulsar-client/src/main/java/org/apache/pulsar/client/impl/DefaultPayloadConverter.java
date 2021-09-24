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

import java.util.Iterator;
import org.apache.pulsar.client.api.EntryContext;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.PayloadConverter;
import org.apache.pulsar.client.api.Schema;

/**
 * The default converter provided for users that want to define their own {@link PayloadConverter}.
 */
public class DefaultPayloadConverter implements PayloadConverter {

    @Override
    public <T> Iterable<Message<T>> convert(EntryContext context, MessagePayload payload, Schema<T> schema) {
        final int numMessages = context.getNumMessages();

        if (context.isBatch()) {
            return () -> new Iterator<Message<T>>() {
                int index = 0;

                @Override
                public boolean hasNext() {
                    final boolean result = (index < numMessages);
                    if (!result) {
                        payload.recycle();
                    }
                    return result;
                }

                @Override
                public Message<T> next() {
                    index++;
                    return context.newSingleMessage(index, numMessages, payload, true, schema);
                }
            };
        } else {
            return () -> new Iterator<Message<T>>() {
                boolean first = true;

                @Override
                public boolean hasNext() {
                    if (!first) {
                        payload.recycle();
                    }
                    return first;
                }

                @Override
                public Message<T> next() {
                    first = false;
                    return context.newMessage(payload, schema);
                }
            };
        }
    }
}
