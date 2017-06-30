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
package org.apache.pulsar.client.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.HexDump;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.util.concurrent.RateLimiter;

/**
 * pulsar-client consume command implementation.
 *
 */
@Parameters(commandDescription = "Consume messages from a specified topic")
public class CmdConsume {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    private static final String MESSAGE_BOUNDARY = "----- got message -----";

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions = new ArrayList<String>();

    @Parameter(names = { "-t", "--subscription-type" }, description = "Subscription type: Exclusive, Shared, Failover.")
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Parameter(names = { "-s", "--subscription-name" }, required = true, description = "Subscription name.")
    private String subscriptionName;

    @Parameter(names = { "-n",
            "--num-messages" }, description = "Number of messages to consume, 0 means to consume forever.")
    private int numMessagesToConsume = 1;

    @Parameter(names = { "--hex" }, description = "Display binary messages in hex.")
    private boolean displayHex = false;

    @Parameter(names = { "-r", "--rate" }, description = "Rate (in msg/sec) at which to consume, "
            + "value 0 means to consume messages as fast as possible.")
    private double consumeRate = 0;

    private String serviceURL = null;
    ClientConfiguration clientConfig;

    public CmdConsume() {
        // Do nothing
    }

    /**
     * Set client configuration.
     *
     */
    public void updateConfig(String serviceURL, ClientConfiguration newConfig) {
        this.serviceURL = serviceURL;
        this.clientConfig = newConfig;
    }

    /**
     * Interprets the message to create a string representation
     *
     * @param message
     *            The message to interpret
     * @param displayHex
     *            Whether to display BytesMessages in hexdump style, ignored for simple text messages
     * @return String representation of the message
     */
    private String interpretMessage(Message message, boolean displayHex) throws IOException {
        byte[] msgData = message.getData();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (!displayHex) {
            return new String(msgData);
        } else {
            HexDump.dump(msgData, 0, out, 0);
            return new String(out.toByteArray());
        }
    }

    /**
     * Run the consume command.
     *
     * @return 0 for success, < 0 otherwise
     */
    public int run() throws PulsarClientException, IOException {
        if (mainOptions.size() != 1)
            throw (new ParameterException("Please provide one and only one topic name."));
        if (this.serviceURL == null || this.serviceURL.isEmpty())
            throw (new ParameterException("Broker URL is not provided."));
        if (this.subscriptionName == null || this.subscriptionName.isEmpty())
            throw (new ParameterException("Subscription name is not provided."));
        if (this.numMessagesToConsume < 0)
            throw (new ParameterException("Number of messages should be zero or positive."));

        String topic = this.mainOptions.get(0);
        int numMessagesConsumed = 0;
        int returnCode = 0;

        try {
            ConsumerConfiguration consumerConf = new ConsumerConfiguration();
            consumerConf.setSubscriptionType(this.subscriptionType);
            PulsarClient client = PulsarClient.create(this.serviceURL, this.clientConfig);
            Consumer consumer = client.subscribe(topic, this.subscriptionName, consumerConf);

            RateLimiter limiter = (this.consumeRate > 0) ? RateLimiter.create(this.consumeRate) : null;
            while (this.numMessagesToConsume == 0 || numMessagesConsumed < this.numMessagesToConsume) {
                if (limiter != null) {
                    limiter.acquire();
                }

                Message msg = consumer.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                    LOG.warn("No message to consume after waiting for 20 seconds.");
                } else {
                    numMessagesConsumed += 1;
                    System.out.println(MESSAGE_BOUNDARY);
                    String output = this.interpretMessage(msg, displayHex);
                    System.out.println(output);
                    consumer.acknowledge(msg);
                }
            }
            client.close();
        } catch (Exception e) {
            LOG.error("Error while consuming messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully consumed", numMessagesConsumed);
        }

        return returnCode;
    }
}
