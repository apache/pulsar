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
package org.apache.pulsar.tests.performance.tools;

import java.nio.ByteBuffer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;

final class TelemetryMessage {
    static final int HEADER_BYTES = Long.BYTES * 4;
    private static final long MEASUREMENT_FLAG = 1;

    static byte[] encode(long deviceId, long sequence, boolean measurement, int size) {
        byte[] payload = new byte[size];
        ByteBuffer.wrap(payload)
                .putLong(deviceId)
                .putLong(sequence)
                .putLong(System.nanoTime())
                .putLong(measurement ? MEASUREMENT_FLAG : 0);
        return payload;
    }

    static Decoded decode(byte[] payload) {
        if (payload.length < HEADER_BYTES) {
            throw new IllegalArgumentException("Telemetry payload is shorter than " + HEADER_BYTES + " bytes");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        return new Decoded(buffer.getLong(), buffer.getLong(), buffer.getLong(),
                (buffer.getLong() & MEASUREMENT_FLAG) != 0);
    }

    /** Keys the device's message: its ID as 8 big-endian bytes, or {@code device-<ID>} with string keys. */
    static TypedMessageBuilder<byte[]> key(TypedMessageBuilder<byte[]> message, long deviceId, boolean stringKeys) {
        return stringKeys ? message.key(stringKey(deviceId))
                : message.keyBytes(ByteBuffer.allocate(Long.BYTES).putLong(deviceId).array());
    }

    /** Whether the message has the key that {@link #key} gives the device's messages. */
    static boolean hasKey(Message<?> message, long deviceId, boolean stringKeys) {
        if (stringKeys) {
            // a string key reaches the consumer as it was sent, not base64-encoded
            return !message.hasBase64EncodedKey() && stringKey(deviceId).equals(message.getKey());
        }
        byte[] key = message.getKeyBytes();
        return key != null && key.length == Long.BYTES && ByteBuffer.wrap(key).getLong() == deviceId;
    }

    private static String stringKey(long deviceId) {
        return "device-" + deviceId;
    }

    private TelemetryMessage() {
    }

    record Decoded(long deviceId, long sequence, long sentNanos, boolean measurement) {
    }
}
