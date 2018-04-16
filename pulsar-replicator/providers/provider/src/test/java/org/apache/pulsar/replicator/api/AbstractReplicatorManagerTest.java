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
package org.apache.pulsar.replicator.api;

import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.replicator.auth.DefaultAuthParamKeyStore;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 */
public class AbstractReplicatorManagerTest {

    @Test
    public void testCreateProducer() throws Exception {
        final String topicName = "persistent://property/cluster/namespace/topic1";
        ReplicatorPolicies policies = new ReplicatorPolicies();
        policies.topicNameMapping = Maps.newHashMap();
        policies.topicNameMapping.put("topic1", "stream-name:us-west-2");
        policies.authParamStorePluginName = DefaultAuthParamKeyStore.class.getName();

        ReplicatorProducerImpl producer = new ReplicatorProducerImpl();
        producer.maxMessageRead = 5;
        AbstractReplicatorManager manager = new MyReplicatorManagerImpl();
        manager.producer = producer;
        manager.topicName = topicName;
        manager.inputConsumer = mockConsumer();
        manager.startProducer(policies);

        Assert.assertEquals(producer.sentMessages, producer.maxMessageRead);
    }

    private Consumer<byte[]> mockConsumer() throws PulsarClientException {
        Consumer<byte[]> inputConsumer = Mockito.mock(Consumer.class);
        Message<byte[]> message = MessageBuilder.create().setContent("".getBytes()).build();
        when(inputConsumer.receiveAsync()).thenReturn(CompletableFuture.completedFuture(message));
        when(inputConsumer.acknowledgeAsync(message)).thenReturn(CompletableFuture.completedFuture(null));
        return inputConsumer;
    }

    static class MyReplicatorManagerImpl extends AbstractReplicatorManager {
        @Override
        public ReplicatorType getType() {
            return ReplicatorType.Kinesis;
        }
        @Override
        protected CompletableFuture<ReplicatorProducer> startProducer(String topicName,
                ReplicatorPolicies replicatorPolicies) {
            return CompletableFuture.completedFuture(producer);
        }
        @Override
        protected void stopProducer() throws Exception {
            // No-op
        }
    }

    static class ReplicatorProducerImpl implements ReplicatorProducer {
        int sentMessages = 0;
        int maxMessageRead = 0;

        @Override
        public CompletableFuture<Void> send(Message message) {
            if (sentMessages >= maxMessageRead) {
                // incomplete future to terminate test
                return new CompletableFuture<Void>();
            }
            sentMessages++;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            // No-op
        }
    }
}
