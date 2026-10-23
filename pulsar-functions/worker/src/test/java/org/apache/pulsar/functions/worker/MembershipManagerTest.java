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
package org.apache.pulsar.functions.worker;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.proto.Assignment;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.apache.pulsar.functions.proto.Instance;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MembershipManagerTest {

    private final WorkerConfig workerConfig;

    @SuppressWarnings("unchecked")
    public MembershipManagerTest() {
        this.workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
    }

    @SuppressWarnings("unchecked")
    private static PulsarClient mockPulsarClient() throws PulsarClientException {
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(mockClient.getCnxPool()).thenReturn(connectionPool);
        ConsumerImpl<byte[]> mockConsumer = mock(ConsumerImpl.class);
        ConsumerBuilder<byte[]> mockConsumerBuilder = mock(ConsumerBuilder.class);

        when(mockConsumerBuilder.topic(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionName(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.property(anyString(), anyString())).thenReturn(mockConsumerBuilder);

        when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);

        when(mockConsumerBuilder.consumerEventListener(any(ConsumerEventListener.class)))
                .thenReturn(mockConsumerBuilder);

        when(mockClient.newConsumer()).thenReturn(mockConsumerBuilder);

        return mockClient;
    }

    private static FunctionMetaData createFunctionMetaData(String tenant, String namespace, String name) {
        FunctionMetaData fmd = new FunctionMetaData();
        fmd.setFunctionDetails().setTenant(tenant).setNamespace(namespace).setName(name);
        return fmd;
    }

    private static FunctionMetaData createFunctionMetaData(String tenant, String namespace, String name,
                                                            int parallelism) {
        FunctionMetaData fmd = new FunctionMetaData();
        fmd.setFunctionDetails().setTenant(tenant).setNamespace(namespace).setName(name).setParallelism(parallelism);
        return fmd;
    }

    private static Assignment createAssignment(String workerId, FunctionMetaData function, int instanceId) {
        Assignment assignment = new Assignment();
        assignment.setWorkerId(workerId);
        assignment.setInstance().setFunctionMetaData().copyFrom(function);
        assignment.getInstance().setInstanceId(instanceId);
        return assignment;
    }

    private static Instance createInstance(FunctionMetaData function, int instanceId) {
        Instance instance = new Instance();
        instance.setFunctionMetaData().copyFrom(function);
        instance.setInstanceId(instanceId);
        return instance;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCheckFailuresNoFailures() throws Exception {
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        PulsarClient pulsarClient = mockPulsarClient();
        ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(true);
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(workerConfig).when(workerService).getWorkerConfig();
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(pulsarAdmin).when(workerService).getFunctionAdmin();

        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        @Cleanup
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class),
                mock(FunctionsManager.class),
                functionMetaDataManager,
                mock(WorkerStatsManager.class),
                mock(ErrorNotifier.class)));
        MembershipManager membershipManager = spy(new MembershipManager(workerService, pulsarClient, pulsarAdmin));

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "host-1", 8000));
        workerInfoList.add(WorkerInfo.of("worker-2", "host-2", 8001));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        FunctionMetaData function1 = createFunctionMetaData("test-tenant", "test-namespace", "func-1");
        FunctionMetaData function2 = createFunctionMetaData("test-tenant", "test-namespace", "func-2");

        List<FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Assignment assignment1 = createAssignment("worker-1", function1, 0);
        Assignment assignment2 = createAssignment("worker-2", function2, 0);

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
        assertEquals(membershipManager.unsignedFunctionDurations.size(), 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCheckFailuresSomeFailures() throws Exception {
        workerConfig.setRescheduleTimeoutMs(30000);
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        PulsarClient pulsarClient = mockPulsarClient();
        ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(true);
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(workerConfig).when(workerService).getWorkerConfig();
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(pulsarAdmin).when(workerService).getFunctionAdmin();

        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        @Cleanup
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class),
                mock(FunctionsManager.class),
                functionMetaDataManager,
                mock(WorkerStatsManager.class),
                mock(ErrorNotifier.class)));

        MembershipManager membershipManager =
                spy(new MembershipManager(workerService, mockPulsarClient(), pulsarAdmin));

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "host-1", 8000));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        FunctionMetaData function1 = createFunctionMetaData("test-tenant", "test-namespace", "func-1");
        FunctionMetaData function2 = createFunctionMetaData("test-tenant", "test-namespace", "func-2");

        List<FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Assignment assignment1 = createAssignment("worker-1", function1, 0);
        Assignment assignment2 = createAssignment("worker-2", function2, 0);

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);

        // Clear any invocations from setup
        Mockito.clearInvocations(functionRuntimeManager, schedulerManager);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
        assertEquals(membershipManager.unsignedFunctionDurations.size(), 1);
        Instance instance = createInstance(function2, 0);
        String instanceId = FunctionCommon.getFullyQualifiedInstanceId(instance);
        assertNotNull(membershipManager.unsignedFunctionDurations.get(instanceId));

        // Clear invocations before second checkFailures
        Mockito.clearInvocations(functionRuntimeManager, schedulerManager);

        membershipManager.unsignedFunctionDurations.put(instanceId,
                membershipManager.unsignedFunctionDurations.get(instanceId) - 30001);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(functionRuntimeManager, times(1)).removeAssignments(
                argThat(assignments -> assignments.contains(assignment2)));

        verify(schedulerManager, times(1)).schedule();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCheckFailuresSomeUnassigned() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setRescheduleTimeoutMs(30000);
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        PulsarClient pulsarClient = mockPulsarClient();
        ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(true);
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(workerConfig).when(workerService).getWorkerConfig();
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(pulsarAdmin).when(workerService).getFunctionAdmin();

        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        @Cleanup
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class),
                mock(FunctionsManager.class),
                functionMetaDataManager,
                mock(WorkerStatsManager.class),
                mock(ErrorNotifier.class)));
        MembershipManager membershipManager =
                spy(new MembershipManager(workerService, mockPulsarClient(), pulsarAdmin));

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "host-1", 8000));
        workerInfoList.add(WorkerInfo.of("worker-2", "host-2", 8001));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        FunctionMetaData function1 = createFunctionMetaData("test-tenant", "test-namespace", "func-1", 1);
        FunctionMetaData function2 = createFunctionMetaData("test-tenant", "test-namespace", "func-2", 1);

        List<FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Assignment assignment1 = createAssignment("worker-1", function1, 0);

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
        assertEquals(membershipManager.unsignedFunctionDurations.size(), 1);
        Instance instance = createInstance(function2, 0);
        String instanceId = FunctionCommon.getFullyQualifiedInstanceId(instance);
        assertNotNull(membershipManager.unsignedFunctionDurations.get(instanceId));

        membershipManager.unsignedFunctionDurations.put(instanceId,
                membershipManager.unsignedFunctionDurations.get(instanceId) - 30001);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(1)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testHeartBeatFunctionWorkerDown() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setRescheduleTimeoutMs(30000);
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        PulsarClient pulsarClient = mockPulsarClient();
        ReaderBuilder<byte[]> readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(true);
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(workerConfig).when(workerService).getWorkerConfig();
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        @Cleanup
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class),
                mock(FunctionsManager.class),
                functionMetaDataManager,
                mock(WorkerStatsManager.class),
                mock(ErrorNotifier.class)));
        MembershipManager membershipManager =
                spy(new MembershipManager(workerService, mockPulsarClient(), pulsarAdmin));

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "host-1", 8000));
        // make worker-2 unavailable
        //workerInfoList.add(WorkerInfo.of("worker-2", "host-2", 8001));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        FunctionMetaData function1 = createFunctionMetaData("test-tenant", "test-namespace", "func-1", 1);

        FunctionMetaData function2 = new FunctionMetaData();
        function2.setFunctionDetails().setParallelism(1)
                .setTenant(SchedulerManager.HEARTBEAT_TENANT)
                .setNamespace(SchedulerManager.HEARTBEAT_NAMESPACE).setName("worker-2");

        List<FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Assignment assignment1 = createAssignment("worker-1", function1, 0);
        Assignment assignment2 = createAssignment("worker-2", function2, 0);

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
        assertEquals(membershipManager.unsignedFunctionDurations.size(), 0);
    }
}
