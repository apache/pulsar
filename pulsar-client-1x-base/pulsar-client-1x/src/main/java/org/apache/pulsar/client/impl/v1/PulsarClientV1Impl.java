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
package org.apache.pulsar.client.impl.v1;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

@SuppressWarnings("deprecation")
public class PulsarClientV1Impl implements PulsarClient {

    private final PulsarClientImpl client;

    public PulsarClientV1Impl(String serviceUrl, ClientConfiguration conf) throws PulsarClientException {
        this.client = new PulsarClientImpl(conf.setServiceUrl(serviceUrl).getConfigurationData().clone());
    }

    @Override
    public void close() throws PulsarClientException {
        client.close();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return client.closeAsync();
    }

    @Override
    public Producer createProducer(final String topic, final ProducerConfiguration conf) throws PulsarClientException {
        if (conf == null) {
            throw new PulsarClientException.InvalidConfigurationException("Invalid null configuration object");
        }

        try {
            return createProducerAsync(topic, conf).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public Producer createProducer(String topic)
            throws PulsarClientException {
        return createProducer(topic, new ProducerConfiguration());
    }

    @Override
    public CompletableFuture<Producer> createProducerAsync(final String topic, final ProducerConfiguration conf) {
        ProducerConfigurationData confData = conf.getProducerConfigurationData().clone();
        confData.setTopicName(topic);
        return client.createProducerAsync(confData).thenApply(p -> new ProducerV1Impl((ProducerImpl<byte[]>) p));
    }

    @Override
    public CompletableFuture<Producer> createProducerAsync(String topic) {
        return createProducerAsync(topic, new ProducerConfiguration());
    }

    @Override
    public Reader createReader(String topic, MessageId startMessageId, ReaderConfiguration conf)
            throws PulsarClientException {
        try {
            return createReaderAsync(topic, startMessageId, conf).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }

    @Override
    public CompletableFuture<Reader> createReaderAsync(String topic, MessageId startMessageId,
            ReaderConfiguration conf) {
        ReaderConfigurationData<byte[]> confData = conf.getReaderConfigurationData().clone();
        confData.setTopicName(topic);
        confData.setStartMessageId(startMessageId);
        return client.createReaderAsync(confData).thenApply(r -> new ReaderV1Impl(r));
    }

    @Override
    public void shutdown() throws PulsarClientException {
        client.shutdown();
    }

    @Override
    public Consumer subscribe(String topic, String subscriptionName) throws PulsarClientException {
        return subscribe(topic, subscriptionName, new ConsumerConfiguration());
    }

    @Override
    public CompletableFuture<Consumer> subscribeAsync(final String topic, final String subscription,
            final ConsumerConfiguration conf) {
        if (conf == null) {
            return FutureUtil.failedFuture(
                    new PulsarClientException.InvalidConfigurationException("Invalid null configuration"));
        }

        ConsumerConfigurationData<byte[]> confData = conf.getConfigurationData().clone();
        confData.getTopicNames().add(topic);
        confData.setSubscriptionName(subscription);
        return client.subscribeAsync(confData).thenApply(c -> new ConsumerV1Impl(c));
    }

    @Override
    public CompletableFuture<Consumer> subscribeAsync(String topic,
            String subscriptionName) {
        return subscribeAsync(topic, subscriptionName, new ConsumerConfiguration());
    }

    @Override
    public Consumer subscribe(String topic, String subscription, ConsumerConfiguration conf)
            throws PulsarClientException {
        try {
            return subscribeAsync(topic, subscription, conf).get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof PulsarClientException) {
                throw (PulsarClientException) t;
            } else {
                throw new PulsarClientException(t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        }
    }
}
