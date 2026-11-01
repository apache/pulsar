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
package org.apache.pulsar.client.cli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.Test;

/**
 * pulsar-shell stops a running command on Ctrl-C by interrupting the thread running it. The command must treat
 * that as the expected end of the run instead of logging it as an error with a stack trace.
 */
public class CmdConsumeInterruptTest {

    @Test
    public void testInterruptedConsumeDoesNotLogError() throws Exception {
        final CountDownLatch receiving = new CountDownLatch(1);
        @SuppressWarnings("unchecked")
        final Consumer<Object> consumer = mock(Consumer.class);
        doAnswer(invocation -> {
            receiving.countDown();
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                return null;
            } catch (InterruptedException e) {
                // What ConsumerImpl.internalReceive does when the receiving thread is interrupted
                Thread.currentThread().interrupt();
                throw new PulsarClientException(e);
            }
        }).when(consumer).receive(anyInt(), any(TimeUnit.class));

        @SuppressWarnings("unchecked")
        final ConsumerBuilder<Object> consumerBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);
        doReturn(consumer).when(consumerBuilder).subscribe();
        final PulsarClient client = mock(PulsarClient.class);
        doReturn(consumerBuilder).when(client).newConsumer(any(Schema.class));
        final ClientBuilder clientBuilder = mock(ClientBuilder.class);
        doReturn(client).when(clientBuilder).build();

        final Properties properties = new Properties();
        properties.setProperty("serviceUrl", "pulsar://localhost:6650");
        final PulsarClientTool tool = new PulsarClientTool(properties) {
            @Override
            ClientBuilder buildV4ClientBuilder(Properties props, Authentication authentication) {
                return clientBuilder;
            }
        };

        final AtomicInteger exitCode = new AtomicInteger(Integer.MIN_VALUE);
        final Thread command = new Thread(() -> exitCode.set(tool.run(new String[]{
                "consume", "-s", "sub", "-n", "0", "persistent://public/default/interrupted"})));
        try (ErrorCapture errors = ErrorCapture.attach(PulsarClientTool.class)) {
            command.start();
            assertTrue(receiving.await(10, TimeUnit.SECONDS));
            command.interrupt();
            command.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(command.isAlive());
            assertEquals(exitCode.get(), -1);
            assertTrue(errors.events().isEmpty(), "unexpected error logs: " + errors.events());
        }
    }

    @Test
    public void testInterruptedWebSocketHandshakeDoesNotLogError() throws Exception {
        // A server that accepts the connection but never answers the WebSocket upgrade, so the command
        // stays blocked waiting for the session to be created.
        final CountDownLatch accepted = new CountDownLatch(1);
        try (ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
            final Thread acceptor = new Thread(() -> {
                try (Socket ignored = server.accept()) {
                    accepted.countDown();
                    Thread.sleep(TimeUnit.SECONDS.toMillis(30));
                } catch (IOException | InterruptedException e) {
                    // closing the server ends the test
                }
            });
            acceptor.setDaemon(true);
            acceptor.start();

            final Properties properties = new Properties();
            properties.setProperty("serviceUrl", "ws://127.0.0.1:" + server.getLocalPort() + "/");
            final PulsarClientTool tool = new PulsarClientTool(properties);

            final AtomicInteger exitCode = new AtomicInteger(Integer.MIN_VALUE);
            final Thread command = new Thread(() -> exitCode.set(tool.run(new String[]{
                    "consume", "-s", "sub", "-n", "0", "persistent://public/default/interrupted"})));
            try (ErrorCapture errors = ErrorCapture.attach(PulsarClientTool.class)) {
                command.start();
                assertTrue(accepted.await(10, TimeUnit.SECONDS));
                command.interrupt();
                command.join(TimeUnit.SECONDS.toMillis(10));
                assertFalse(command.isAlive());
                assertEquals(exitCode.get(), -1);
                assertTrue(errors.events().isEmpty(), "unexpected error logs: " + errors.events());
            } finally {
                acceptor.interrupt();
            }
        }
    }

    /** Collects the ERROR events logged through one logger while attached. */
    private static final class ErrorCapture extends AbstractAppender implements AutoCloseable {
        private final List<String> events = Collections.synchronizedList(new ArrayList<>());
        private final String loggerName;
        private final LoggerConfig loggerConfig;
        private final LoggerContext context;

        private ErrorCapture(String loggerName, LoggerConfig loggerConfig, LoggerContext context) {
            super("ErrorCapture-" + System.nanoTime(), null, null, true, Property.EMPTY_ARRAY);
            this.loggerName = loggerName;
            this.loggerConfig = loggerConfig;
            this.context = context;
        }

        static ErrorCapture attach(Class<?> loggerOwner) {
            final LoggerContext context = (LoggerContext) LogManager.getContext(false);
            final LoggerConfig loggerConfig = context.getConfiguration().getLoggerConfig(loggerOwner.getName());
            final ErrorCapture capture = new ErrorCapture(loggerOwner.getName(), loggerConfig, context);
            capture.start();
            loggerConfig.addAppender(capture, Level.ERROR, null);
            context.updateLoggers();
            return capture;
        }

        @Override
        public void append(LogEvent event) {
            if (loggerName.equals(event.getLoggerName()) && event.getLevel().isMoreSpecificThan(Level.ERROR)) {
                events.add(event.getMessage().getFormattedMessage());
            }
        }

        List<String> events() {
            return new ArrayList<>(events);
        }

        @Override
        public void close() {
            loggerConfig.removeAppender(getName());
            context.updateLoggers();
            stop();
        }
    }
}
