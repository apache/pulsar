/*
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
package org.apache.pulsar.io.cassandra.producers;

import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

@SuppressWarnings({"unchecked", "rawtypes"})
@Slf4j
public abstract class InputTopicProducerThread<T> implements Runnable {

    private Random rnd = new Random();
    final String inputTopic;
    final String brokerUrl;
    PulsarClient client;
    Producer<T> producer;

    public InputTopicProducerThread(String brokerUrl, String inputTopic) {
        this.brokerUrl = brokerUrl;
        this.inputTopic = inputTopic;
    }

    @Override
    public void run() {
        for (int idx = 0; idx < 100; idx++) {
            try {
                getProducer().newMessage().key(getKey()).value(getValue()).send();
            } catch (PulsarClientException e) {
                log.error("Unable to connect to Pulsar", e);
            }
        }
    }

    String getKey() {
        Integer i = Integer.valueOf(rnd.nextInt(999999));
        return i.toString();
    }

    abstract T getValue();

    abstract Schema<T> getSchema();

    private PulsarClient getPulsarClient() throws PulsarClientException {
        if (client == null) {
            client = PulsarClient.builder()
                    .serviceUrl(brokerUrl)
                    .build();
        }

        return client;
    }

    private Producer<T> getProducer() throws PulsarClientException {
        if (producer == null) {
            producer = getPulsarClient().newProducer(getSchema())
                    .topic(inputTopic)
                    .create();
        }

        return producer;
    }

}
