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
package org.apache.pulsar.functions.instance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.async.AsyncLoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * The log topic's appender when a function instance closes: the events logged before the close reach the log topic,
 * also when async loggers hand them to the appender after the close started.
 */
public class LogAppenderTest {

    @Test
    public void testEventsQueuedBeforeTheCloseReachTheLogTopic() throws Exception {
        RecordingAppender recording = new RecordingAppender();
        AsyncLoggerContext context = start(new AsyncLoggerContext("log-appender-async-test"), recording);
        List<String> published = new CopyOnWriteArrayList<>();
        try {
            LogAppender logAppender = newLogAppender(published);
            JavaInstanceRunnable.setupLogTopicAppender(context, logAppender);
            Logger logger = context.getLogger("function");
            // Hold the background thread on an earlier event, so that the last event waits in the queue
            logger.info("block");
            assertTrue(recording.blocked.await(10, TimeUnit.SECONDS));
            logger.info("last");

            Thread close = new Thread(() -> {
                JavaInstanceRunnable.removeLogTopicAppender(context, logAppender);
                logAppender.stop();
            });
            close.start();
            // Release the background thread once the close waits for it, or has detached the appender without waiting
            Awaitility.await().until(() -> close.getState() == Thread.State.TIMED_WAITING
                    || close.getState() == Thread.State.TERMINATED);
            recording.release.countDown();
            close.join(TimeUnit.SECONDS.toMillis(10));

            assertFalse(close.isAlive());
            assertEquals(published, List.of("block", "last"));
            assertDetached(context, logAppender);
        } finally {
            recording.release.countDown();
            context.stop(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainWithSynchronousLoggers() throws Exception {
        LoggerContext context = start(new LoggerContext("log-appender-sync-test"), new RecordingAppender());
        List<String> published = new CopyOnWriteArrayList<>();
        try {
            LogAppender logAppender = newLogAppender(published);
            JavaInstanceRunnable.setupLogTopicAppender(context, logAppender);
            context.getLogger("function").info("last");

            assertTrue(logAppender.drain(context, 0));
            JavaInstanceRunnable.removeLogTopicAppender(context, logAppender);
            logAppender.stop();

            assertEquals(published, List.of("last"));
            assertDetached(context, logAppender);
        } finally {
            context.stop(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDrainGivesUpAfterTheTimeout() throws Exception {
        RecordingAppender recording = new RecordingAppender();
        AsyncLoggerContext context = start(new AsyncLoggerContext("log-appender-timeout-test"), recording);
        List<String> published = new CopyOnWriteArrayList<>();
        try {
            LogAppender logAppender = newLogAppender(published);
            JavaInstanceRunnable.setupLogTopicAppender(context, logAppender);
            Logger logger = context.getLogger("function");
            logger.info("block");
            assertTrue(recording.blocked.await(10, TimeUnit.SECONDS));

            assertFalse(logAppender.drain(context, 100));

            // The drain's event, which the background thread handles after the timeout, reaches no other appender
            recording.release.countDown();
            logger.info("after");
            Awaitility.await().until(() -> recording.received.contains("after"));
            assertEquals(recording.received, List.of("block", "after"));
            assertEquals(published, List.of("block", "after"));
        } finally {
            recording.release.countDown();
            context.stop(10, TimeUnit.SECONDS);
        }
    }

    private static <T extends LoggerContext> T start(T context, RecordingAppender recording) {
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        builder.setConfigurationName(context.getName());
        builder.add(builder.newRootLogger(Level.INFO));
        context.start(builder.build(false));
        recording.start();
        context.getConfiguration().getRootLogger().addAppender(recording, null, null);
        context.updateLoggers();
        return context;
    }

    // An appender for the log topic, whose producer adds the messages that it publishes to the list
    @SuppressWarnings("unchecked")
    private static LogAppender newLogAppender(List<String> published) throws Exception {
        TypedMessageBuilder<byte[]> message = mock(TypedMessageBuilder.class, RETURNS_SELF);
        when(message.value(any())).thenAnswer(invocation -> {
            published.add(new String((byte[]) invocation.getArgument(0), StandardCharsets.UTF_8));
            return message;
        });
        when(message.sendAsync()).thenReturn(CompletableFuture.completedFuture(null));
        Producer<byte[]> producer = mock(Producer.class);
        when(producer.newMessage()).thenReturn(message);
        when(producer.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class, RETURNS_SELF);
        when(producerBuilder.create()).thenReturn(producer);
        PulsarClient client = mock(PulsarClient.class);
        when(client.newProducer()).thenReturn(producerBuilder);
        LogAppender logAppender = new LogAppender(client, "log-topic", "tenant/namespace/function", "0");
        logAppender.start();
        return logAppender;
    }

    private static void assertDetached(LoggerContext context, LogAppender logAppender) {
        Configuration config = context.getConfiguration();
        assertFalse(config.getRootLogger().getAppenders().containsKey(logAppender.getName()));
        assertTrue(config.getLoggers().values().stream()
                .noneMatch(loggerConfig -> loggerConfig.getAppenders().containsKey(logAppender.getName())));
    }

    // Records the messages of the events that it receives, and holds the thread that hands it the event "block" until
    // released
    private static final class RecordingAppender extends AbstractAppender {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        RecordingAppender() {
            super("recording", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            received.add(message);
            if (message.equals("block")) {
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
