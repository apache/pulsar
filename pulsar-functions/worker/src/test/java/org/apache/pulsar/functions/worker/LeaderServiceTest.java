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
package org.apache.pulsar.functions.worker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class LeaderServiceTest {

    private final WorkerConfig workerConfig;
    private LeaderService leaderService;
    private PulsarClientImpl mockClient;
    AtomicReference<ConsumerEventListener> listenerHolder;
    private ConsumerImpl mockConsumer;
    private FunctionAssignmentTailer functionAssignmentTailer;
    private SchedulerManager schedulerManager;
    private FunctionRuntimeManager functionRuntimeManager;
    private FunctionMetaDataManager functionMetadataManager;
    private CompletableFuture metadataManagerInitFuture;
    private CompletableFuture runtimeManagerInitFuture;
    private CompletableFuture readToTheEndAndExitFuture;
    private MembershipManager membershipManager;

    public LeaderServiceTest() {
        this.workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setWorkerPort(1234);
    }

    @BeforeMethod
    public void setup() throws PulsarClientException {
        mockClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(mockClient.getCnxPool()).thenReturn(connectionPool);
        mockConsumer = mock(ConsumerImpl.class);
        ConsumerBuilder<byte[]> mockConsumerBuilder = mock(ConsumerBuilder.class);

        when(mockConsumerBuilder.topic(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionName(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.property(anyString(), anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.consumerName(anyString())).thenReturn(mockConsumerBuilder);

        when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);
        WorkerService workerService = mock(WorkerService.class);
        doReturn(workerConfig).when(workerService).getWorkerConfig();

        listenerHolder = new AtomicReference<>();
        when(mockConsumerBuilder.consumerEventListener(any(ConsumerEventListener.class))).thenAnswer(invocationOnMock -> {

            ConsumerEventListener listener = invocationOnMock.getArgument(0);
            listenerHolder.set(listener);

            return mockConsumerBuilder;
        });

        when(mockClient.newConsumer()).thenReturn(mockConsumerBuilder);

        schedulerManager = mock(SchedulerManager.class);

        functionAssignmentTailer = mock(FunctionAssignmentTailer.class);
        readToTheEndAndExitFuture = mock(CompletableFuture.class);
        when(functionAssignmentTailer.triggerReadToTheEndAndExit()).thenReturn(readToTheEndAndExitFuture);

        functionRuntimeManager = mock(FunctionRuntimeManager.class);
        functionMetadataManager = mock(FunctionMetaDataManager.class);

        metadataManagerInitFuture = mock(CompletableFuture.class);
        runtimeManagerInitFuture = mock(CompletableFuture.class);

        when(functionMetadataManager.getIsInitialized()).thenReturn(metadataManagerInitFuture);
        when(functionRuntimeManager.getIsInitialized()).thenReturn(runtimeManagerInitFuture);

        membershipManager = mock(MembershipManager.class);

        leaderService = spy(new LeaderService(workerService, mockClient, functionAssignmentTailer, schedulerManager,
          functionRuntimeManager, functionMetadataManager,  membershipManager, ErrorNotifier.getDefaultImpl()));
        leaderService.start();
    }

    @Test
    public void testLeaderService() throws Exception {
        MessageId messageId = new MessageIdImpl(1, 2, -1);
        when(schedulerManager.getLastMessageProduced()).thenReturn(messageId);

        assertFalse(leaderService.isLeader());
        verify(mockClient, times(1)).newConsumer();

        listenerHolder.get().becameActive(mockConsumer, 0);
        assertTrue(leaderService.isLeader());

        verify(functionMetadataManager, times(1)).getIsInitialized();
        verify(metadataManagerInitFuture, times(1)).get();
        verify(functionRuntimeManager, times(1)).getIsInitialized();
        verify(runtimeManagerInitFuture, times(1)).get();

        verify(functionMetadataManager, times(1)).acquireExclusiveWrite(any());
        verify(schedulerManager, times(1)).acquireExclusiveWrite(any());

        verify(functionAssignmentTailer, times(1)).triggerReadToTheEndAndExit();
        verify(functionAssignmentTailer, times(1)).close();
        verify(schedulerManager, times((1))).initialize(any());

        listenerHolder.get().becameInactive(mockConsumer, 0);
        assertFalse(leaderService.isLeader());

        verify(functionAssignmentTailer, times(1)).startFromMessage(messageId);
        verify(schedulerManager, times(1)).close();
        verify(functionMetadataManager, times(1)).giveupLeadership();
    }

    @Test
    public void testLeaderServiceNoNewScheduling() throws Exception {
        when(schedulerManager.getLastMessageProduced()).thenReturn(null);

        assertFalse(leaderService.isLeader());
        verify(mockClient, times(1)).newConsumer();

        listenerHolder.get().becameActive(mockConsumer, 0);
        assertTrue(leaderService.isLeader());

        verify(functionMetadataManager, times(1)).acquireExclusiveWrite(any());
        verify(schedulerManager, times(1)).acquireExclusiveWrite(any());

        verify(functionAssignmentTailer, times(1)).triggerReadToTheEndAndExit();
        verify(readToTheEndAndExitFuture, times(1)).get();
        verify(functionAssignmentTailer, times(1)).close();
        verify(schedulerManager, times((1))).initialize(any());

        listenerHolder.get().becameInactive(mockConsumer, 0);
        assertFalse(leaderService.isLeader());

        verify(functionAssignmentTailer, times(1)).start();
        verify(schedulerManager, times(1)).close();
        verify(functionMetadataManager, times(1)).giveupLeadership();
    }

    @Test
    public void testAcquireScheduleManagerExclusiveProducerNotLeaderAnymore() throws Exception {
        MessageId messageId = new MessageIdImpl(1, 2, -1);
        when(schedulerManager.getLastMessageProduced()).thenReturn(messageId);

        assertFalse(leaderService.isLeader());
        verify(mockClient, times(1)).newConsumer();

        // acquire exclusive producer failed because no leader anymore
        when(schedulerManager.acquireExclusiveWrite(any())).thenThrow(new WorkerUtils.NotLeaderAnymore());

        listenerHolder.get().becameActive(mockConsumer, 0);
        // should have failed to become leader
        assertFalse(leaderService.isLeader());

        verify(functionMetadataManager, times(1)).getIsInitialized();
        verify(metadataManagerInitFuture, times(1)).get();
        verify(functionRuntimeManager, times(1)).getIsInitialized();
        verify(runtimeManagerInitFuture, times(1)).get();

        verify(schedulerManager, times(1)).acquireExclusiveWrite(any());
        verify(functionMetadataManager, times(0)).acquireExclusiveWrite(any());

        verify(functionAssignmentTailer, times(0)).triggerReadToTheEndAndExit();
        verify(functionAssignmentTailer, times(0)).close();
        verify(schedulerManager, times((0))).initialize(any());

        listenerHolder.get().becameInactive(mockConsumer, 0);
        assertFalse(leaderService.isLeader());

        verify(functionAssignmentTailer, times(0)).startFromMessage(messageId);
        verify(functionAssignmentTailer, times(0)).start();
        verify(schedulerManager, times(0)).close();
        verify(functionMetadataManager, times(0)).giveupLeadership();
    }

    @Test
    public void testAcquireFunctionMetadataManagerExclusiveProducerNotLeaderAnymore() throws Exception {
        MessageId messageId = new MessageIdImpl(1, 2, -1);
        when(schedulerManager.getLastMessageProduced()).thenReturn(messageId);

        assertFalse(leaderService.isLeader());
        verify(mockClient, times(1)).newConsumer();

        // acquire exclusive producer failed because no leader anymore
        when(functionMetadataManager.acquireExclusiveWrite(any())).thenThrow(new WorkerUtils.NotLeaderAnymore());

        listenerHolder.get().becameActive(mockConsumer, 0);
        // should have failed to become leader
        assertFalse(leaderService.isLeader());

        verify(functionMetadataManager, times(1)).getIsInitialized();
        verify(metadataManagerInitFuture, times(1)).get();
        verify(functionRuntimeManager, times(1)).getIsInitialized();
        verify(runtimeManagerInitFuture, times(1)).get();

        verify(schedulerManager, times(1)).acquireExclusiveWrite(any());
        verify(functionMetadataManager, times(1)).acquireExclusiveWrite(any());

        verify(functionAssignmentTailer, times(0)).triggerReadToTheEndAndExit();
        verify(functionAssignmentTailer, times(0)).close();
        verify(schedulerManager, times((0))).initialize(any());

        listenerHolder.get().becameInactive(mockConsumer, 0);
        assertFalse(leaderService.isLeader());

        verify(functionAssignmentTailer, times(0)).startFromMessage(messageId);
        verify(functionAssignmentTailer, times(0)).start();
        verify(schedulerManager, times(0)).close();
        verify(functionMetadataManager, times(0)).giveupLeadership();
    }

}
