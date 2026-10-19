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
package org.apache.pulsar.client.impl.v5;

import java.util.Optional;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.classification.InterfaceAudience;

/**
 * Bridges between the v4 and V5 client types for Pulsar runtimes that support both clients, such as the
 * Pulsar Functions runtime.
 *
 * <p>This is not a public API: it exposes existing package-private adapters and may change without notice.
 */
@InterfaceAudience.Private
public final class V5Interop {

    private V5Interop() {
    }

    /** Adapts a v4 schema to the V5 schema interface. */
    public static <T> Schema<T> toV5Schema(org.apache.pulsar.client.api.Schema<T> v4Schema) {
        return SchemaAdapter.toV5(v4Schema);
    }

    /** Adapts a V5 schema to the v4 schema interface. */
    public static <T> org.apache.pulsar.client.api.Schema<T> toV4Schema(Schema<T> v5Schema) {
        return SchemaAdapter.toV4(v5Schema);
    }

    /**
     * Returns the v4 message that a V5 message received by this client wraps.
     *
     * <p>The v4 message carries the details that the V5 message interface does not expose, such as the schema
     * version, the reader schema and the encryption context. Its topic is the segment topic, not the scalable
     * topic, and its message id is the segment's message id.
     *
     * @return the wrapped v4 message, or empty if the message was not created by this client
     */
    public static <T> Optional<org.apache.pulsar.client.api.Message<T>> v4Message(Message<T> v5Message) {
        if (v5Message instanceof MessageV5<T> messageV5) {
            return Optional.of(messageV5.v4Message());
        }
        return Optional.empty();
    }
}
