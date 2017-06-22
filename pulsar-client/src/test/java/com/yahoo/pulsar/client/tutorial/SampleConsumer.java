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
package com.yahoo.pulsar.client.tutorial;

import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;

public class SampleConsumer {
    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        PulsarClient pulsarClient = PulsarClient.create("http://localhost:8080");

        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic", "my-subscriber-name");
        Message msg = null;

        for (int i = 0; i < 100; i++) {
            msg = consumer.receive();
            // do something
            System.out.println("Received: " + new String(msg.getData()));
        }

        // Acknowledge the consumption of all messages at once
        consumer.acknowledge(msg);
        pulsarClient.close();
    }
}
