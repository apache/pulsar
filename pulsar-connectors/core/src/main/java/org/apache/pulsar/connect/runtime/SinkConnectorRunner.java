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
package org.apache.pulsar.connect.runtime;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.io.FileSystems;
import org.apache.pulsar.connect.api.sink.SinkConnector;
import org.apache.pulsar.connect.config.ConnectorConfiguration;
import org.apache.pulsar.connect.util.Bytes;
import org.apache.pulsar.connect.util.ConfigUtils;
import org.apache.pulsar.connect.util.InstanceBuilder;
import org.apache.pulsar.connect.util.PropertiesValidator;
import org.apache.pulsar.connect.util.PulsarUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class SinkConnectorRunner extends ConnectorRunner {
    private static final Logger LOG = LoggerFactory.getLogger(SinkConnectorRunner.class);

    private static final long DEFAULT_ACK_INTERVAL_MB = 5;

    private final SinkConnector connector;
    private final Properties properties;

    private final long commitIntervalBytesMb;

    private boolean keepGoing = true;

    private SinkConnectorRunner(SinkConnector connector, Properties properties) {
        this.connector = connector;
        this.properties = properties;
        commitIntervalBytesMb =
                ConfigUtils.getLong(properties,
                        ConnectorConfiguration.KEY_COMMIT_INTERVAL_MB, DEFAULT_ACK_INTERVAL_MB) * Bytes.MB;
    }

    @Override
    public void run() {
        try (PulsarClient client = createClient(properties)) {
            //
            connector.initialize(properties);

            final String topic = getProperty(ConnectorConfiguration.KEY_TOPIC);
            final String subscription = getProperty(ConnectorConfiguration.KEY_SUBSCRIPTION);
            final ConsumerConfiguration configuration =
                    new ConsumerConfiguration().setSubscriptionType(SubscriptionType.Failover);

            // create a subscription and start processing messages
            try (Consumer consumer = client.subscribe(topic, subscription, configuration)) {
                LOG.info("Running sink connector {} for topic {} and subscription {}",
                        connector.getClass().getSimpleName(), topic, subscription);
                LOG.info("acknowledgement interval {} Mb", Bytes.toMb(commitIntervalBytesMb));

                runSinkConnector(consumer);

            } catch (PulsarClientException pce) {
                LOG.info("unable to create subscribe to topic {} "
                        + "with subscription {}", topic, subscription);
                throw new ConnectorExecutionException(pce);
            }
        } catch (PulsarClientException pce) {
            throw new ConnectorExecutionException(pce);
        } finally {
            connector.close();
        }
    }

    private void runSinkConnector(Consumer consumer) {
        long bytesProcessed = 0;
        final List<MessageId> messageIds = new ArrayList<>();
        Message currentMessage;
        while (keepGoing) {
            try {
                currentMessage = consumer.receive();
                messageIds.add(currentMessage.getMessageId());

                connector.processMessage(currentMessage);
                bytesProcessed += currentMessage.getData().length;

                // TODO add an option for a flush interval
                // is it time to acknowledge?
                if (bytesProcessed >= commitIntervalBytesMb) {
                    connector.commit();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("acknowledging {} messages", messageIds.size());
                    }
                    acknowledge(consumer, messageIds);
                    bytesProcessed = 0;

                    // clear ids since we just acknowledged
                    messageIds.clear();
                }
            } catch (Exception ex) {
                throw new ConnectorExecutionException(ex);
            }
        }
    }

    private void acknowledge(Consumer consumer, List<MessageId> messageIds)
            throws PulsarClientException {
        // TODO wait for these to finish
        for (MessageId messageId : messageIds) {
            consumer.acknowledgeAsync(messageId);
        }
    }

    private String getProperty(String key) {
        return properties.getProperty(key);
    }

    private PulsarClient createClient(Properties properties) throws PulsarClientException {
        return PulsarUtils.createClient(properties);
    }

    public static SinkConnectorRunner fromProperties(Properties properties) {
        PropertiesValidator.validateThrowIfMissingKeys(properties,
                ConnectorConfiguration.KEY_SUBSCRIPTION);

        FileSystems.register(properties);

        final SinkConnector connector;
        try {
            final String sinkConnectorClass =
                    properties.getProperty(ConnectorConfiguration.KEY_CONNECTOR);
            connector = InstanceBuilder
                    .ofType(SinkConnector.class)
                    .fromClassName(sinkConnectorClass)
                    .build();
        } catch (ClassNotFoundException e) {
            throw new ConnectorExecutionException(e);
        }

        return new SinkConnectorRunner(connector, properties);
    }
}
