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
package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.channel.DefaultEventLoop;
import io.netty.util.internal.DefaultPriorityQueue;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AbstractReplicatorTest {

    @Test
    public void testRetryStartProducerStoppedByTopicRemove() throws Exception {
        final String localCluster = "localCluster";
        final String remoteCluster = "remoteCluster";
        final String topicName = "remoteTopicName";
        final String replicatorPrefix = "pulsar.repl";
        @Cleanup("shutdownNow")
        final DefaultEventLoop eventLoopGroup = new DefaultEventLoop();
        // Mock services.
        final ServiceConfiguration pulsarConfig = mock(ServiceConfiguration.class);
        final PulsarService pulsar = mock(PulsarService.class);
        final BrokerService broker = mock(BrokerService.class);
        final Topic localTopic = mock(Topic.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        final PulsarClientImpl localClient = mock(PulsarClientImpl.class);
        when(localClient.getCnxPool()).thenReturn(connectionPool);
        final PulsarClientImpl remoteClient = mock(PulsarClientImpl.class);
        when(remoteClient.getCnxPool()).thenReturn(connectionPool);
        final ProducerConfigurationData producerConf = new ProducerConfigurationData();
        final ProducerBuilderImpl producerBuilder = mock(ProducerBuilderImpl.class);
        final var topics = new ConcurrentHashMap<String, CompletableFuture<Optional<Topic>>>();
        when(broker.executor()).thenReturn(eventLoopGroup);
        when(broker.getTopics()).thenReturn(topics);
        when(remoteClient.newProducer(any(Schema.class))).thenReturn(producerBuilder);
        when(broker.pulsar()).thenReturn(pulsar);
        when(pulsar.getClient()).thenReturn(localClient);
        when(pulsar.getConfiguration()).thenReturn(pulsarConfig);
        when(pulsarConfig.getReplicationProducerQueueSize()).thenReturn(100);
        when(localTopic.getName()).thenReturn(topicName);
        when(producerBuilder.topic(any())).thenReturn(producerBuilder);
        when(producerBuilder.messageRoutingMode(any())).thenReturn(producerBuilder);
        when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
        when(producerBuilder.sendTimeout(anyInt(), any())).thenReturn(producerBuilder);
        when(producerBuilder.maxPendingMessages(anyInt())).thenReturn(producerBuilder);
        when(producerBuilder.producerName(anyString())).thenReturn(producerBuilder);
        when(producerBuilder.getConf()).thenReturn(producerConf);
        // Mock create producer fail.
        when(producerBuilder.create()).thenThrow(new RuntimeException("mocked ex"));
        when(producerBuilder.createAsync())
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("mocked ex")));
        // Make race condition: "retry start producer" and "close replicator".
        final ReplicatorInTest replicator = new ReplicatorInTest(localCluster, localTopic, remoteCluster, topicName,
                replicatorPrefix, broker, remoteClient);
        replicator.startProducer();
        replicator.terminate();

        // Verify task will done.
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            AtomicInteger taskCounter = new AtomicInteger();
            CountDownLatch checkTaskFinished = new CountDownLatch(1);
            eventLoopGroup.execute(() -> {
                synchronized (replicator) {
                    LinkedBlockingQueue taskQueue = WhiteboxImpl.getInternalState(eventLoopGroup, "taskQueue");
                    DefaultPriorityQueue scheduledTaskQueue =
                            WhiteboxImpl.getInternalState(eventLoopGroup, "scheduledTaskQueue");
                    taskCounter.set(taskQueue.size() + scheduledTaskQueue.size());
                    checkTaskFinished.countDown();
                }
            });
            checkTaskFinished.await();
            Assert.assertEquals(taskCounter.get(), 0);
        });
    }

    private static class ReplicatorInTest extends AbstractReplicator {

        public ReplicatorInTest(String localCluster, Topic localTopic, String remoteCluster, String remoteTopicName,
                                String replicatorPrefix, BrokerService brokerService,
                                PulsarClientImpl replicationClient) throws PulsarServerException {
            super(localCluster, localTopic, remoteCluster, remoteTopicName, replicatorPrefix, brokerService,
                    replicationClient);
        }

        @Override
        protected String getProducerName() {
            return "pulsar.repl.producer";
        }

        @Override
        protected void setProducerAndTriggerReadEntries(Producer<byte[]> producer) {

        }

        @Override
        protected Position getReplicatorReadPosition() {
            return PositionFactory.EARLIEST;
        }

        @Override
        public ReplicatorStatsImpl computeStats() {
            return null;
        }

        @Override
        public ReplicatorStatsImpl getStats() {
            return null;
        }

        @Override
        public void updateRates() {

        }

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public long getNumberOfEntriesInBacklog() {
            return 0;
        }

        @Override
        protected void disableReplicatorRead() {

        }
    }
}
