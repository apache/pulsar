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

package org.apache.pulsar.io.rocketmq.source;

import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.rocketmq.RocketMQSource;
import org.apache.pulsar.io.rocketmq.RocketMQSourceConfig;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;


public class RocketMQSourceTest {

    private static class DummySource extends RocketMQSource {

    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private static <T extends Exception> void expectThrows(Class<T> expectedType, String expectedMessage, ThrowingRunnable runnable) {
        try {
            runnable.run();
            Assert.fail();
        } catch (Throwable e) {
            if (expectedType.isInstance(e)) {
                T ex = expectedType.cast(e);
                assertEquals(expectedMessage, ex.getMessage());
                return;
            }
            throw new AssertionError("Unexpected exception type, expected " + expectedType.getSimpleName() + " but got " + e);
        }
        throw new AssertionError("Expected exception");
    }

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("rocketmqSourceConfig.yaml");
        RocketMQSourceConfig config = RocketMQSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
        assertEquals("localhost:6667", config.getNamesrvAddr());
        assertEquals("test", config.getTopic());
        assertEquals("test-pulsar-io-consumer-group", config.getConsumerGroup());
        assertEquals("CLUSTERING", config.getMessageModel());
        assertEquals("*", config.getTag());
        assertEquals(Integer.parseInt("32"), config.getBatchSize());
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
