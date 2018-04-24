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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.connect.core.Record;

/**
 * Abstract class that implements message api and connect record api.
 */
public abstract class MessageRecordImpl<T, M extends MessageId> implements Message<T>, Record<T> {

    protected M messageId;
    private Consumer<T> consumer;

    public void setConsumer(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void ack() {
        if (null != consumer && null != messageId) {
            consumer.acknowledgeAsync(messageId);
        }
    }

    @Override
    public void fail() {
        // no-op
    }
}
