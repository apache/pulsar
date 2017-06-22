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

import java.io.IOException;

import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.api.Producer;

public class ContinuousProducer {
    public static void main(String[] args) throws PulsarClientException, InterruptedException, IOException {
        PulsarClient pulsarClient = PulsarClient.create("http://127.0.0.1:8080");

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic");

        while (true) {
            try {
                producer.send("my-message".getBytes());
                Thread.sleep(1000);

            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }

        pulsarClient.close();
    }
}
