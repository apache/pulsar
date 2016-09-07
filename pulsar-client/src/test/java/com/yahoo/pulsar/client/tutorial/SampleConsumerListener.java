/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.tutorial;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageListener;

public class SampleConsumerListener {
    public static void main(String[] args) throws PulsarClientException, InterruptedException, IOException {
        PulsarClient pulsarClient = PulsarClient.create("http://localhost:8080");

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setMessageListener(new MessageListener() {
            public void received(Consumer consumer, Message msg) {
                log.info("Received message: {}", msg);
                consumer.acknowledgeAsync(msg);
            }
        });

        pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic", "my-subscriber-name", conf);

        // Block main thread
        System.in.read();

        pulsarClient.close();
    }

    private static final Logger log = LoggerFactory.getLogger(SampleConsumerListener.class);
}
