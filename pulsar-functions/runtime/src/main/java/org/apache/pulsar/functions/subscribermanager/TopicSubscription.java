/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Spawner is the module responsible for running one particular instance servicing one
 * function. It is responsible for starting/stopping the instance and passing data to the
 * instance and getting the results back.
 */
package org.apache.pulsar.functions.subscribermanager;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.functions.runtime.container.FunctionContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class TopicSubscription {
    private static final Logger log = LoggerFactory.getLogger(TopicSubscription.class);
    String topicName;
    String subscriptionId;
    ConcurrentMap<String, FunctionContainer> subscriberMap;
    private Thread thread;
    private PulsarClient client;
    private Consumer consumer;
    private ResultsProcessor resultsProcessor;
    private volatile boolean running;

    TopicSubscription(String topicName, String subscriptionId, PulsarClient client,
                      ResultsProcessor resultsProcessor) {
        this.topicName = topicName;
        this.subscriptionId = subscriptionId;
        subscriberMap = new ConcurrentHashMap<>();
        this.client = client;
        this.resultsProcessor = resultsProcessor;
        running = false;
    }

    public void start() throws Exception {
        String subscriptionName = "fn-" + subscriptionId + "-" + topicName + "-subscriber";
        consumer = client.subscribe(topicName, subscriptionName,
                new ConsumerConfiguration().setSubscriptionType(SubscriptionType.Shared));
        running = true;
        thread = new Thread(() -> {
            try {
                while (running) {
                    // Wait for a message
                    Message msg = consumer.receive(1000, TimeUnit.MILLISECONDS);
                    if (null == msg) {
                        continue;
                    }
                    log.info("Received message {}", msg);
                    String messageId = convertMessageIdToString(msg.getMessageId());
                    for (FunctionContainer subscriber : subscriberMap.values()) {
                        subscriber.sendMessage(topicName, messageId, msg.getData())
                                .thenApply(result -> resultsProcessor.handleResult(subscriber, result));
                    }

                    // Acknowledge the message so that it can be deleted by broker
                    consumer.acknowledge(msg);
                }
            } catch (Exception ex) {
                log.error("Got exception while dealing with topic " + topicName, ex);
            }
        }, subscriptionName);
        thread.start();
    }

    public void stop() {
        if (thread != null) {
            running = false;
            try {
                thread.join();
            } catch (InterruptedException ex) {
                log.error("Got Interrupted while waiting for the thread to join! Ignoring...");
            }
            consumer.closeAsync();
            thread = null;
        }
    }

    void addSubscriber(FunctionContainer subscriber) throws Exception {
        if (subscriberMap.isEmpty()) {
            start();
        }
        subscriberMap.putIfAbsent(subscriber.getId(), subscriber);
    }

    void removeSubscriber(String subscriberId) {
        subscriberMap.remove(subscriberId);
        if (subscriberMap.isEmpty()) {
            stop();
        }
    }

    private String convertMessageIdToString(MessageId messageId) {
        return messageId.toByteArray().toString();
    }
}
