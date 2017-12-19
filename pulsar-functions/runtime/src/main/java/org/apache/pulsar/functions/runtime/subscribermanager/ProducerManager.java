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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ProducerManager {
    private PulsarClient client;
    private ConcurrentMap<String, Producer> producerMap;

    ProducerManager(PulsarClient client) {
        this.client = client;
        producerMap = new ConcurrentHashMap<>();
    }

    private void createProducer(String topicName) {
        try {
            Producer producer = client.createProducer(topicName);
            producerMap.putIfAbsent(topicName, producer);
        } catch (Exception ex) {
            throw new RuntimeException("Exception occured while creating producer", ex);
        }
    }

    public void publish(String topicName, byte[] data) throws Exception{
        if (!producerMap.containsKey(topicName)) {
            createProducer(topicName);
        }
        producerMap.get(topicName).send(data);
    }
}
