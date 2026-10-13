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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.ErrorHandler;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.DefaultErrorHandler;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionPolicy;
import org.apache.pulsar.client.impl.v5.V5Interop;
import org.apache.pulsar.functions.instance.v5.V5ProducerAdapter;

/**
 * LogAppender class that is used to send log statements from Pulsar Functions logger
 * to a log topic.
 */
public class LogAppender implements Appender {

    private static final String LOG_LEVEL = "loglevel";
    private static final String INSTANCE = "instance";
    private static final String FQN = "fqn";

    private PulsarClient pulsarClient;
    private Supplier<org.apache.pulsar.client.api.v5.PulsarClient> pulsarClientV5;
    private String logTopic;
    private String fqn;
    private String instance;
    private State state;
    private ErrorHandler errorHandler;
    private Producer<byte[]> producer;

    public LogAppender(PulsarClient pulsarClient, String logTopic, String fqn, String instance) {
        this.pulsarClient = pulsarClient;
        this.logTopic = logTopic;
        this.fqn = fqn;
        this.instance = instance;
        this.errorHandler = new DefaultErrorHandler(this);
    }

    /**
     * Creates an appender that publishes to the log topic with the V5 client.
     */
    public LogAppender(Supplier<org.apache.pulsar.client.api.v5.PulsarClient> pulsarClientV5, String logTopic,
                       String fqn, String instance) {
        this((PulsarClient) null, logTopic, fqn, instance);
        this.pulsarClientV5 = pulsarClientV5;
    }

    @Override
    public void append(LogEvent logEvent) {
        producer.newMessage()
                .value(logEvent.getMessage().getFormattedMessage().getBytes(StandardCharsets.UTF_8))
                .property(LOG_LEVEL, logEvent.getLevel().name())
                .property(INSTANCE, instance)
                .property(FQN, fqn)
                .sendAsync();
    }

    @Override
    public String getName() {
        return fqn;
    }

    @Override
    public Layout<? extends Serializable> getLayout() {
        return null;
    }

    @Override
    public boolean ignoreExceptions() {
        return false;
    }

    @Override
    public ErrorHandler getHandler() {
        return errorHandler;
    }

    @Override
    public void setHandler(ErrorHandler errorHandler) {
        if (errorHandler == null) {
            throw new RuntimeException("The log error handler cannot be set to null");
        }
        if (isStarted()) {
            throw new RuntimeException("The log error handler cannot be changed once the appender is started");
        }
        this.errorHandler = errorHandler;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public void initialize() {
        this.state = State.INITIALIZED;
    }

    @Override
    public void start() {
        this.state = State.STARTING;
        try {
            if (pulsarClientV5 != null) {
                producer = createProducerV5();
                this.state = State.STARTED;
                return;
            }
            producer = pulsarClient.newProducer()
                    .topic(logTopic)
                    .blockIfQueueFull(false)
                    .enableBatching(true)
                    .compressionType(CompressionType.LZ4)
                    .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                    .property("function", fqn)
                    .create();
        } catch (Exception e) {
            throw new RuntimeException("Error starting LogTopic Producer for function " + fqn, e);
        }
        this.state = State.STARTED;
    }

    private Producer<byte[]> createProducerV5() throws Exception {
        // the same settings as the v4 producer
        return new V5ProducerAdapter<>(pulsarClientV5.get()
                .newProducer(V5Interop.toV5Schema(Schema.BYTES))
                .topic(logTopic)
                .blockIfQueueFull(false)
                .batchingPolicy(BatchingPolicy.builder().maxPublishDelay(Duration.ofMillis(100)).build())
                .compressionPolicy(CompressionPolicy.of(org.apache.pulsar.client.api.v5.config.CompressionType.LZ4))
                .property("function", fqn)
                .create(), Schema.BYTES);
    }

    @Override
    public void stop() {
        this.state = State.STOPPING;
        if (producer != null) {
            producer.closeAsync();
            producer = null;
        }
        this.state = State.STOPPED;
    }

    @Override
    public boolean isStarted() {
        return state == State.STARTED;
    }

    @Override
    public boolean isStopped() {
        return state == State.STOPPED;
    }
}
