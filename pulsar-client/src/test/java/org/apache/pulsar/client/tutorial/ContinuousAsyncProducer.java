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
package org.apache.pulsar.client.tutorial;

import java.io.IOException;

import lombok.Cleanup;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class ContinuousAsyncProducer {
    public static void main(String[] args) throws InterruptedException, IOException {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("http://127.0.0.1:8080").build();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-tenant/my-ns/my-topic")
                .blockIfQueueFull(true).create();

        while (true) {
            producer.sendAsync("my-message".getBytes());
        }
    }
}
