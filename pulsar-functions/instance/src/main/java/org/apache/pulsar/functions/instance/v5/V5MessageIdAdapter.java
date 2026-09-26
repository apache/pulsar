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
package org.apache.pulsar.functions.instance.v5;

import java.util.Objects;
import org.apache.pulsar.client.api.MessageId;

/**
 * Presents a V5 message id through the v4 {@link MessageId} interface, so that the Functions runtime and the
 * v4-typed user API can carry ids of messages sent or received with the V5 client.
 *
 * <p>{@link #toByteArray()} returns the V5 serialization; it can be restored with
 * {@link org.apache.pulsar.client.api.v5.MessageId#fromByteArray(byte[])}, not with the v4 counterpart.
 */
public final class V5MessageIdAdapter implements MessageId {

    private static final long serialVersionUID = 1L;

    private final transient org.apache.pulsar.client.api.v5.MessageId messageId;

    public V5MessageIdAdapter(org.apache.pulsar.client.api.v5.MessageId messageId) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
    }

    /** Returns the wrapped V5 message id. */
    public org.apache.pulsar.client.api.v5.MessageId v5MessageId() {
        return messageId;
    }

    @Override
    public byte[] toByteArray() {
        return messageId.toByteArray();
    }

    @Override
    public int compareTo(MessageId other) {
        if (other instanceof V5MessageIdAdapter adapter) {
            return messageId.compareTo(adapter.messageId);
        }
        throw new IllegalArgumentException("Cannot compare a V5 message id with " + other);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof V5MessageIdAdapter adapter && messageId.equals(adapter.messageId);
    }

    @Override
    public int hashCode() {
        return messageId.hashCode();
    }

    @Override
    public String toString() {
        return messageId.toString();
    }
}
