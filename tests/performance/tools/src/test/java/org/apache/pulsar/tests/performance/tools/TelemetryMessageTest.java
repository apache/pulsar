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

import static org.assertj.core.api.Assertions.assertThat;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.testng.annotations.Test;

public class TelemetryMessageTest {
    @Test
    public void preservesMeasurementMarker() {
        TelemetryMessage.Decoded warmup = TelemetryMessage.decode(
                TelemetryMessage.encode(3, 7, false, TelemetryMessage.HEADER_BYTES));
        TelemetryMessage.Decoded measurement = TelemetryMessage.decode(
                TelemetryMessage.encode(3, 8, true, TelemetryMessage.HEADER_BYTES + 10));

        assertThat(warmup.deviceId()).isEqualTo(3);
        assertThat(warmup.sequence()).isEqualTo(7);
        assertThat(warmup.measurement()).isFalse();
        assertThat(measurement.deviceId()).isEqualTo(3);
        assertThat(measurement.sequence()).isEqualTo(8);
        assertThat(measurement.measurement()).isTrue();
    }

    @Test
    public void matchesTheKeyOfTheDevice() {
        for (boolean stringKeys : new boolean[] {false, true}) {
            Map<String, Object> key = new HashMap<>();
            TelemetryMessage.key(builder(key), 42, stringKeys);
            assertThat(key).containsOnlyKeys(stringKeys ? "key" : "keyBytes");
            Message<?> message = received(key);
            assertThat(TelemetryMessage.hasKey(message, 42, stringKeys)).isTrue();
            assertThat(TelemetryMessage.hasKey(message, 43, stringKeys)).isFalse();
            assertThat(TelemetryMessage.hasKey(message, 42, !stringKeys)).isFalse();
        }
    }

    // A message builder that records the key that it's given
    @SuppressWarnings("unchecked")
    private static TypedMessageBuilder<byte[]> builder(Map<String, Object> key) {
        return (TypedMessageBuilder<byte[]>) Proxy.newProxyInstance(TypedMessageBuilder.class.getClassLoader(),
                new Class<?>[] {TypedMessageBuilder.class}, (proxy, method, args) -> {
                    key.put(method.getName(), args[0]);
                    return proxy;
                });
    }

    // The message that a consumer receives with the recorded key, where a binary key arrives base64-encoded
    private static Message<?> received(Map<String, Object> key) {
        byte[] keyBytes = (byte[]) key.get("keyBytes");
        String stringKey = keyBytes != null ? Base64.getEncoder().encodeToString(keyBytes) : (String) key.get("key");
        return (Message<?>) Proxy.newProxyInstance(Message.class.getClassLoader(), new Class<?>[] {Message.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "hasBase64EncodedKey" -> keyBytes != null;
                    case "getKey" -> stringKey;
                    case "getKeyBytes" -> keyBytes != null ? keyBytes : stringKey.getBytes(StandardCharsets.UTF_8);
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
