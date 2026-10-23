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

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import org.apache.pulsar.client.api.MessageId;

/**
 * Presents a V5 message id through the v4 {@link MessageId} interface, so that the Functions runtime and the
 * v4-typed user API can carry ids of messages sent or received with the V5 client.
 *
 * <p>{@link #toByteArray()} returns the V5 serialization; it can be restored with
 * {@link org.apache.pulsar.client.api.v5.MessageId#fromByteArray(byte[])}, not with the v4 counterpart. Java
 * serialization, which the v4 interface supports, also goes through that byte form.
 */
public final class V5MessageIdAdapter implements MessageId {

    private static final long serialVersionUID = 1L;

    private final transient org.apache.pulsar.client.api.v5.MessageId messageId;

    /** The serialized form: the V5 message id bytes, restored through the V5 client. */
    private record SerializedForm(byte[] data) implements Serializable {
        @Serial
        private Object readResolve() throws ObjectStreamException {
            try {
                return new V5MessageIdAdapter(org.apache.pulsar.client.api.v5.MessageId.fromByteArray(data));
            } catch (IOException e) {
                InvalidObjectException invalid = new InvalidObjectException("Invalid V5 message id");
                invalid.initCause(e);
                throw invalid;
            }
        }
    }

    public V5MessageIdAdapter(org.apache.pulsar.client.api.v5.MessageId messageId) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
    }

    @Serial
    private Object writeReplace() {
        return new SerializedForm(messageId.toByteArray());
    }

    @Serial
    private void readObject(ObjectInputStream in) throws InvalidObjectException {
        throw new InvalidObjectException("Deserialized through SerializedForm");
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
