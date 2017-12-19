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

import org.apache.pulsar.client.api.Codec;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TypedMessage;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

class TypedMessageImpl<T> implements TypedMessage<T> {
    private final Message message;
    private final FutureTask<T> expanded;

    public TypedMessageImpl(Message message, Codec<T> codec) {
        this.message = message;
        this.expanded = new FutureTask<>(() ->
                codec.decode(message.getData())
        );
    }

    @Override
    public T getMessage() {
        try {
            return expanded.get();
        } catch (Exception ignored) {
        }
        return null;
    }

    @Override
    public Map<String, String> getProperties() {
        return message.getProperties();
    }

    @Override
    public boolean hasProperty(String name) {
        return message.hasProperty(name);
    }

    @Override
    public String getProperty(String name) {
        return message.getProperty(name);
    }

    @Override
    public byte[] getData() {
        return message.getData();
    }

    @Override
    public MessageId getMessageId() {
        return message.getMessageId();
    }

    @Override
    public long getPublishTime() {
        return message.getPublishTime();
    }

    @Override
    public long getEventTime() {
        return message.getEventTime();
    }

    @Override
    public boolean hasKey() {
        return message.hasKey();
    }

    @Override
    public String getKey() {
        return message.getKey();
    }
}
