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
package org.apache.pulsar.spark;

import java.io.Serializable;
import java.util.Set;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SparkStreamingPulsarReceiver extends Receiver<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingPulsarReceiver.class);

    private String serviceUrl;
    private ConsumerConfigurationData<byte[]> conf;
    private Authentication authentication;
    private PulsarClient pulsarClient;
    private Consumer<byte[]> consumer;

    public SparkStreamingPulsarReceiver(
        String serviceUrl,
        ConsumerConfigurationData<byte[]> conf,
        Authentication authentication) {
        this(StorageLevel.MEMORY_AND_DISK_2(), serviceUrl, conf, authentication);
    }

    public SparkStreamingPulsarReceiver(StorageLevel storageLevel,
        String serviceUrl,
        ConsumerConfigurationData<byte[]> conf,
        Authentication authentication) {
        super(storageLevel);

        checkNotNull(serviceUrl, "serviceUrl must not be null");
        checkNotNull(conf, "ConsumerConfigurationData must not be null");
        checkArgument(conf.getTopicNames().size() > 0, "TopicNames must be set a value.");
        checkNotNull(conf.getSubscriptionName(), "SubscriptionName must not be null");

        this.serviceUrl = serviceUrl;
        this.authentication = authentication;

        if (conf.getAckTimeoutMillis() == 0) {
            conf.setAckTimeoutMillis(60000);
        }
        if (conf.getMessageListener() == null) {
            conf.setMessageListener((MessageListener & Serializable) (consumer, msg) -> {
                try {
                    store(msg.getData());
                    consumer.acknowledgeAsync(msg);
                } catch (Exception e) {
                    LOG.error("Failed to store a message : {}", e.getMessage());
                }
            });
        }
        this.conf = conf;
    }

    public void onStart() {
        try {
            Set<String> topicNames = conf.getTopicNames();
            String[] topicNamesArray = new String[topicNames.size()];
            topicNames.toArray(topicNamesArray);
            pulsarClient = PulsarClient.builder().serviceUrl(serviceUrl).authentication(authentication).build();
            consumer = pulsarClient.newConsumer().topic(topicNamesArray).subscriptionName(conf.getSubscriptionName())
                .messageListener(this.conf.getMessageListener()).subscribe();
        } catch (PulsarClientException e) {
            LOG.error("Failed to start subscription : {}", e.getMessage());
            restart("Restart a consumer");
        }
    }

    public void onStop() {
        try {
            if (consumer != null) {
                consumer.close();
            }
            if (pulsarClient != null) {
                pulsarClient.close();
            }
        } catch (PulsarClientException e) {
            LOG.error("Failed to close client : {}", e.getMessage());
        }
    }
}