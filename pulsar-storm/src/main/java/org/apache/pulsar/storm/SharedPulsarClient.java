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
package org.apache.pulsar.storm;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedPulsarClient {
    private static final Logger LOG = LoggerFactory.getLogger(SharedPulsarClient.class);
    private static final ConcurrentMap<String, SharedPulsarClient> instances = new ConcurrentHashMap<>();

    private final String componentId;
    private final PulsarClientImpl client;
    private final AtomicInteger counter = new AtomicInteger();

    private Consumer<byte[]> consumer;
    private Reader<byte[]> reader;
    private Producer<byte[]> producer;

    private SharedPulsarClient(String componentId, ClientConfigurationData clientConf)
            throws PulsarClientException {
        this.client = new PulsarClientImpl(clientConf);
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
    public static SharedPulsarClient get(String componentId, ClientConfigurationData clientConf)
            throws PulsarClientException {
        AtomicReference<PulsarClientException> exception = new AtomicReference<PulsarClientException>();
        instances.computeIfAbsent(componentId, pulsarClient -> {
            SharedPulsarClient sharedPulsarClient = null;
            try {
                sharedPulsarClient = new SharedPulsarClient(componentId, clientConf);
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

    public PulsarClientImpl getClient() {
        counter.incrementAndGet();
        return client;
    }

    public Consumer<byte[]> getSharedConsumer(ConsumerConfigurationData<byte[]> consumerConf)
            throws PulsarClientException {
        counter.incrementAndGet();
        synchronized (this) {
            if (consumer == null) {
                try {
                    consumer = client.subscribeAsync(consumerConf).join();
                } catch (CompletionException e) {
                    throw (PulsarClientException) e.getCause();
                }
                LOG.info("[{}] Created a new Pulsar Consumer on {}", componentId, consumerConf.getSingleTopic());
            } else {
                LOG.info("[{}] Using a shared consumer on {}", componentId, consumerConf.getSingleTopic());
            }
        }
        return consumer;
    }

    public Reader<byte[]> getSharedReader(ReaderConfigurationData<byte[]> readerConf) throws PulsarClientException {
        counter.incrementAndGet();
        synchronized (this) {
            if (reader == null) {
                try {
                    reader = client.createReaderAsync(readerConf).join();
                } catch (CompletionException e) {
                    throw (PulsarClientException) e.getCause();
                }
                LOG.info("[{}] Created a new Pulsar reader on {}", componentId, readerConf.getTopicName());
            } else {
                LOG.info("[{}] Using a shared reader on {}", componentId, readerConf.getTopicName());
            }
        }
        return reader;
    }

    public Producer<byte[]> getSharedProducer(ProducerConfigurationData producerConf) throws PulsarClientException {
        counter.incrementAndGet();
        synchronized (this) {
            if (producer == null) {
                try {
                    producer = client.createProducerAsync(producerConf).join();
                } catch (CompletionException e) {
                    throw (PulsarClientException) e.getCause();
                }
                LOG.info("[{}] Created a new Pulsar Producer on {}", componentId, producerConf.getTopicName());
            } else {
                LOG.info("[{}] Using a shared producer on {}", componentId, producerConf.getTopicName());
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
