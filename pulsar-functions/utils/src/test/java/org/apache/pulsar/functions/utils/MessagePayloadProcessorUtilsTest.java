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
package org.apache.pulsar.functions.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.MessagePayloadProcessorConfig;
import org.apache.pulsar.functions.proto.Function;
import org.testng.annotations.Test;

public class MessagePayloadProcessorUtilsTest {

    // Dummy implementation
    public static class TestProcessor implements MessagePayloadProcessor {
        public TestProcessor() {}
        public TestProcessor(Map<String, Object> config) {}

        @Override
        public <T> void process(MessagePayload payload, MessagePayloadContext context, Schema<T> schema,
                                Consumer<Message<T>> messageConsumer) throws Exception {

        }
    }

    public static class NoDefaultConstructorProcessor implements MessagePayloadProcessor {
        public NoDefaultConstructorProcessor(Map<String, Object> config) {}

        @Override
        public <T> void process(MessagePayload payload, MessagePayloadContext context, Schema<T> schema,
                                Consumer<Message<T>> messageConsumer) throws Exception {
        }
    }

    public static class NoMapConstructorProcessor implements MessagePayloadProcessor {
        public NoMapConstructorProcessor() {}

        @Override
        public <T> void process(MessagePayload payload, MessagePayloadContext context, Schema<T> schema,
                                Consumer<Message<T>> messageConsumer) throws Exception {
        }
    }

    @Test
    public void testGetInstanceWithDefaultConstructor() {
        MessagePayloadProcessor processor = MessagePayloadProcessorUtils
                .getMessagePayloadProcessorInstance(TestProcessor.class.getName(), null, getClass().getClassLoader());

        assertNotNull(processor);
        assertTrue(processor instanceof TestProcessor);
    }

    @Test
    public void testGetInstanceWithMapConstructor() {
        Map<String, Object> config = new HashMap<>();
        config.put("key", "value");

        MessagePayloadProcessor processor = MessagePayloadProcessorUtils
                .getMessagePayloadProcessorInstance(TestProcessor.class.getName(), config, getClass().getClassLoader());

        assertNotNull(processor);
        assertTrue(processor instanceof TestProcessor);
    }

    @Test(expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = ".*Failed to load message payload processor class.*")
    public void testClassNotFound() {
        MessagePayloadProcessorUtils.getMessagePayloadProcessorInstance(
                "non.existent.Class", null, getClass().getClassLoader());
    }

    @Test(expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = ".*does not have default constructor.*")
    public void testNoDefaultConstructor() {
        MessagePayloadProcessorUtils.getMessagePayloadProcessorInstance(
                NoDefaultConstructorProcessor.class.getName(), null, getClass().getClassLoader());
    }

    @Test(expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = ".*does not have constructor accepts map.*")
    public void testNoMapConstructor() {
        Map<String, Object> config = new HashMap<>();
        config.put("a", "b");

        MessagePayloadProcessorUtils.getMessagePayloadProcessorInstance(
                NoMapConstructorProcessor.class.getName(), config, getClass().getClassLoader());
    }

    @Test
    public void testConvertFromSpecReturnsNullIfEmpty() {
        Function.MessagePayloadProcessorSpec spec = Function.MessagePayloadProcessorSpec.newBuilder()
                .setClassName("")
                .build();

        assertNull(MessagePayloadProcessorUtils.convertFromSpec(spec));
    }

    @Test
    public void testConvertFromSpecValid() {
        String json = "{\"threshold\": 10}";
        Function.MessagePayloadProcessorSpec spec = Function.MessagePayloadProcessorSpec.newBuilder()
                .setClassName("com.example.MyProcessor")
                .setConfigs(json)
                .build();

        MessagePayloadProcessorConfig config = MessagePayloadProcessorUtils.convertFromSpec(spec);
        assertNotNull(config);
        assertEquals(config.getClassName(), "com.example.MyProcessor");
        assertEquals(config.getConfig().get("threshold"), 10.0); // Gson uses Double for numbers
    }

    @Test
    public void testConvertToSpecWithConfig() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("foo", "bar");

        MessagePayloadProcessorConfig config = MessagePayloadProcessorConfig.builder()
                .className("test.Foo")
                .config(conf)
                .build();

        Function.MessagePayloadProcessorSpec spec = MessagePayloadProcessorUtils.convert(config);
        assertEquals(spec.getClassName(), "test.Foo");
        assertTrue(spec.getConfigs().contains("foo"));
    }

    @Test
    public void testConvertToSpecWithoutConfig() {
        MessagePayloadProcessorConfig config = MessagePayloadProcessorConfig.builder()
                .className("test.Foo")
                .config(null)
                .build();

        Function.MessagePayloadProcessorSpec spec = MessagePayloadProcessorUtils.convert(config);
        assertEquals(spec.getClassName(), "test.Foo");
        assertEquals(spec.getConfigs(), "");
    }
}