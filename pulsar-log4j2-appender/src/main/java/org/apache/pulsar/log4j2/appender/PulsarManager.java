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
package org.apache.pulsar.log4j2.appender;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractManager;
import org.apache.logging.log4j.core.config.Property;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public class PulsarManager extends AbstractManager {

    static Supplier<ClientBuilder> PULSAR_CLIENT_BUILDER = PulsarClient::builder;

    private PulsarClient client;
    private Producer<byte[]> producer;

    private final String serviceUrl;
    private final String topic;
    private final String key;
    private final boolean syncSend;

    public PulsarManager(final LoggerContext loggerContext,
                         final String name,
                         final String serviceUrl,
                         final String topic,
                         final boolean syncSend,
                         final Property[] properties,
                         final String key) {
        super(loggerContext, name);
        this.serviceUrl = Objects.requireNonNull(serviceUrl, "serviceUrl");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.syncSend = syncSend;
        this.key = key;
    }

    @Override
    public boolean releaseSub(final long timeout, final TimeUnit timeUnit) {
        if (producer != null) {
            try {
                producer.closeAsync().get(timeout, timeUnit);
            } catch (Exception e) {
                // exceptions on closing
                LOGGER.warn("Failed to close producer within {} milliseconds",
                    timeUnit.toMillis(timeout), e);
            }
        }
        return true;
    }

    public void send(final byte[] msg)  {
        if (producer != null) {
            String newKey = null;

            if(key != null && key.contains("${")) {
                newKey = getLoggerContext().getConfiguration().getStrSubstitutor().replace(key);
            } else if (key != null) {
                newKey = key;
            }


            TypedMessageBuilder<byte[]> messageBuilder = producer.newMessage()
                    .value(msg);

            if (newKey != null) {
                messageBuilder.key(newKey);
            }

            if (syncSend) {
                try {
                    messageBuilder.send();
                } catch (PulsarClientException e) {
                    LOGGER.error("Unable to write to Pulsar in appender [" + getName() + "]", e);
                }
            } else {
                messageBuilder.sendAsync()
                    .exceptionally(cause -> {
                        LOGGER.error("Unable to write to Pulsar in appender [" + getName() + "]", cause);
                        return null;
                    });
            }
        }
    }

    public void startup() throws Exception {
        try {
            client = PULSAR_CLIENT_BUILDER.get()
                .serviceUrl(serviceUrl)
                .build();
            ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                .topic(topic)
                .producerName("pulsar-log4j2-appender-" + topic)
                .blockIfQueueFull(false);
            if (syncSend) {
                // disable batching for sync send
                producerBuilder = producerBuilder.enableBatching(false);
            } else {
                // enable batching in 10 ms for async send
                producerBuilder = producerBuilder
                    .enableBatching(true)
                    .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS);
            }
            producer = producerBuilder.create();
        } catch (Exception t) {
            LOGGER.error("Failed to start pulsar manager", t);
            throw t;
        }
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getTopic() {
        return topic;
    }

}
