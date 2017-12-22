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
/**
 * Spawner is the module responsible for running one particular instance servicing one
 * function. It is responsible for starting/stopping the instance and passing data to the
 * instance and getting the results back.
 */
package org.apache.pulsar.functions.runtime.subscribermanager;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.runtime.container.FunctionContainer;
import org.apache.pulsar.functions.stats.FunctionStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicSubscription {

    @Data
    @RequiredArgsConstructor
    @Getter
    private static class ContainerWithStats {

        private final FunctionContainer container;
        private final FunctionStatsImpl stats;

    }

    private static final Logger log = LoggerFactory.getLogger(TopicSubscription.class);
    private final String topicName;
    private final String subscriptionId;
    private final ConcurrentMap<String, ContainerWithStats> subscriberMap;
    private final PulsarClientImpl client;
    private final ConsumerConfiguration consumerConfiguration;
    private Consumer consumer;
    private volatile boolean running;

    TopicSubscription(String topicName,
                      String subscriptionId,
                      PulsarClient client,
                      ResultsProcessor resultsProcessor) {
        this.topicName = topicName;
        this.subscriptionId = subscriptionId;
        subscriberMap = new ConcurrentHashMap<>();
        this.client = (PulsarClientImpl) client;
        this.consumerConfiguration = new ConsumerConfiguration().setSubscriptionType(SubscriptionType.Shared);
        consumerConfiguration.setMessageListener((consumer, msg) -> {
            try {
                String messageId = convertMessageIdToString(msg.getMessageId());
                for (ContainerWithStats subscriber : subscriberMap.values()) {
                    long processedAt = System.nanoTime();
                    subscriber.stats.incrementProcess();
                    subscriber.container.sendMessage(topicName, messageId, msg.getData())
                            .thenApply(result -> resultsProcessor.handleResult(
                                subscriber.container,
                                result,
                                subscriber.stats,
                                processedAt))
                            .exceptionally(cause -> {
                                subscriber.stats.incrementProcessFailure();
                                return true;
                            });
                }
                // Acknowledge the message so that it can be deleted by broker
                consumer.acknowledgeAsync(msg);
            } catch (Exception ex) {
                log.error("Got exception while dealing with topic " + topicName, ex);
            }
        });
        running = false;
    }

    public void start() throws Exception {
        String subscriptionName = "fn-" + subscriptionId + "-" + topicName + "-subscriber";
        consumer = client.subscribe(topicName, subscriptionName, consumerConfiguration);
        running = true;
    }

    public void stop() {
        if (running) {
            running = false;
            if (null != consumer) {
                consumer.closeAsync();
            }
        }
    }

    void addSubscriber(FunctionContainer container) throws Exception {
        if (subscriberMap.isEmpty()) {
            start();
        }
        ContainerWithStats subscriber = new ContainerWithStats(
            container,
            new FunctionStatsImpl(
                container.getId(),
                client.getConfiguration().getStatsIntervalSeconds(),
                client.timer())
        );

        subscriberMap.putIfAbsent(subscriber.container.getId(), subscriber);
    }

    void removeSubscriber(String subscriberId) {
        subscriberMap.remove(subscriberId);
        if (subscriberMap.isEmpty()) {
            stop();
        }
    }

    private static String convertMessageIdToString(MessageId messageId) {
        return messageId.toByteArray().toString();
    }
}
