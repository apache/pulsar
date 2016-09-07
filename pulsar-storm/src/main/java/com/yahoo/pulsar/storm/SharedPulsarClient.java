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
package com.yahoo.pulsar.storm;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;

public class SharedPulsarClient {
    private static final Logger LOG = LoggerFactory.getLogger(SharedPulsarClient.class);
    private static final ConcurrentMap<String, SharedPulsarClient> instances = Maps.newConcurrentMap();

    private final String componentId;
    private final PulsarClient client;
    private final AtomicInteger counter = new AtomicInteger();

    private Consumer consumer;
    private Producer producer;

    private SharedPulsarClient(String componentId, String serviceUrl, ClientConfiguration clientConf)
            throws PulsarClientException {
        this.client = PulsarClient.create(serviceUrl, clientConf);
        this.componentId = componentId;
    }

    /**
     * Provides a shared pulsar client that is shared across all different tasks in the same component. Different
     * components will not share the pulsar client since they can have different configurations.
     *
     * @param componentId
     *            the id of the spout/bolt
     * @param serviceUrl
     * @param clientConf
     * @return
     * @throws PulsarClientException
     */
    public static SharedPulsarClient get(String componentId, String serviceUrl, ClientConfiguration clientConf)
            throws PulsarClientException {
        AtomicReference<PulsarClientException> exception = new AtomicReference<PulsarClientException>();
        instances.computeIfAbsent(componentId, pulsarClient -> {
            SharedPulsarClient sharedPulsarClient = null;
            try {
                sharedPulsarClient = new SharedPulsarClient(componentId, serviceUrl, clientConf);
                LOG.info("[{}] Created a new Pulsar Client.", componentId);
            } catch (PulsarClientException e) {
                exception.set(e);
            }
            return sharedPulsarClient;
        });
        if (exception.get() != null) {
            throw exception.get();
        }
        return instances.get(componentId);
    }

    public PulsarClient getClient() {
        counter.incrementAndGet();
        return client;
    }

    public Consumer getSharedConsumer(String topic, String subscription, ConsumerConfiguration consumerConf)
            throws PulsarClientException {
        counter.incrementAndGet();
        synchronized (this) {
            if (consumer == null) {
                consumer = client.subscribe(topic, subscription, consumerConf);
                LOG.info("[{}] Created a new Pulsar Consumer on {}", componentId, topic);
            } else {
                LOG.info("[{}] Using a shared consumer on {}", componentId, topic);
            }
        }
        return consumer;
    }

    public Producer getSharedProducer(String topic, ProducerConfiguration producerConf) throws PulsarClientException {
        counter.incrementAndGet();
        synchronized (this) {
            if (producer == null) {
                producer = client.createProducer(topic, producerConf);
                LOG.info("[{}] Created a new Pulsar Producer on {}", componentId, topic);
            } else {
                LOG.info("[{}] Using a shared producer on {}", componentId, topic);
            }
        }
        return producer;
    }

    public void close() throws PulsarClientException {
        if (counter.decrementAndGet() <= 0) {
            if (client != null) {
                client.close();
                instances.remove(componentId);
                LOG.info("[{}] Closed Pulsar Client", componentId);
            }
        }
    }
}
