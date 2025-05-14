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
package org.apache.pulsar.functions.api.examples;


import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.Schema;

@Slf4j
public class TestPayloadProcessor implements MessagePayloadProcessor {
    public TestPayloadProcessor() {
        log.info("TestPayloadProcessor constructor without configs");
    }

    public TestPayloadProcessor(Map<String, Object> conf) {
        String configs = conf.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", "));
        log.info("TestPayloadProcessor constructor with configs {}", configs);
    }

    @Override
    public <T> void process(MessagePayload payload, MessagePayloadContext context, Schema<T> schema,
                            Consumer<Message<T>> messageConsumer) throws Exception {
        log.info("Processing message using TestPayloadProcessor");
        if (context.isBatch()) {
            final int numMessages = context.getNumMessages();
            for (int i = 0; i < numMessages; i++) {
                messageConsumer.accept(context.getMessageAt(i, numMessages, payload, true, schema));
            }
        } else {
            messageConsumer.accept(context.asSingleMessage(payload, schema));
        }
    }
}
