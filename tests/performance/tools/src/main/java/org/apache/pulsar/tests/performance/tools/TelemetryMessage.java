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

    private TelemetryMessage() {
    }

    record Decoded(long deviceId, long sequence, long sentNanos, boolean measurement) {
    }
}
