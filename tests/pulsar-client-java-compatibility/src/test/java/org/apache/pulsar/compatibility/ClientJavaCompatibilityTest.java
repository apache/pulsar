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

package org.apache.pulsar.compatibility;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.Map;
import java.util.Properties;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.cli.PulsarClientTool;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.testng.annotations.Test;

public class ClientJavaCompatibilityTest {
    // Compile real user implementations against the complete API graph, not just the API jar.
    private static class Echo implements Function<String, String> {
        @Override
        public String process(String input, Context context) {
            return input;
        }
    }

    private static class StringSink implements Sink<String> {
        private String value;

        @Override
        public void open(Map<String, Object> config, SinkContext context) {
        }

        @Override
        public void write(Record<String> record) {
            value = record.getValue();
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void loadClientImplementationsOnClientJavaVersion() {
        assertEquals(Runtime.version().feature(), Integer.parseInt(System.getProperty("pulsarClientJavaVersion")));
        assertNotNull(PulsarClient.builder());
        assertNotNull(org.apache.pulsar.client.api.v5.PulsarClient.builder());
        assertNotNull(PulsarAdmin.builder());
        assertEquals(Schema.STRING.decode(Schema.STRING.encode("java17")), "java17");
    }

    @Test
    public void loadClientToolsOnClientJavaVersion() {
        assertNotNull(new PulsarClientTool(new Properties()));
        ProducerMessage message = new ProducerMessage();
        message.setPayload("java17");
        assertEquals(message.getPayload(), "java17");
    }

    @Test
    public void runUserFunctionAndSinkOnClientJavaVersion() {
        assertEquals(new Echo().process("java17", null), "java17");
        StringSink sink = new StringSink();
        sink.write(() -> "java17");
        assertEquals(sink.value, "java17");
    }
}
