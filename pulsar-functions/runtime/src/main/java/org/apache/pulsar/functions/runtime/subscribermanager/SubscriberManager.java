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

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.functions.runtime.container.FunctionContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SubscriberManager {
    private static final Logger log = LoggerFactory.getLogger(SubscriberManager.class);
    private String workerId;
    private String pulsarBrokerRootUrl;
    // Map from topicName to its subscription info
    Map<String, TopicSubscription> topicSubscriptionMap;
    private PulsarClient pulsarClient;
    private ResultsProcessor resultsProcessor;

    public SubscriberManager(String workerId, String pulsarBrokerRootUrl) throws Exception {
        this.workerId = workerId;
        this.pulsarBrokerRootUrl = pulsarBrokerRootUrl;
        topicSubscriptionMap = new HashMap<>();
        pulsarClient = PulsarClient.create(pulsarBrokerRootUrl);
        resultsProcessor = new ResultsProcessor(pulsarClient);
    }

    public void addSubscriber(String topicName, FunctionContainer subscriber) throws Exception {
        if (!topicSubscriptionMap.containsKey(topicName)) {
            topicSubscriptionMap.put(topicName, new TopicSubscription(topicName, workerId, pulsarClient, resultsProcessor));
        }
        topicSubscriptionMap.get(topicName).addSubscriber(subscriber);
    }

    public void removeSubscriber(String topicName, String subscriberId) {
        if (topicSubscriptionMap.containsKey(topicName)) {
            topicSubscriptionMap.get(topicName).removeSubscriber(subscriberId);
        }
    }
}