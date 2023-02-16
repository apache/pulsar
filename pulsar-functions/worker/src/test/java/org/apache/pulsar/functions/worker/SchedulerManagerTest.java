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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.Assignment;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class SchedulerManagerTest {

    private SchedulerManager schedulerManager;
    private FunctionMetaDataManager functionMetaDataManager;
    private FunctionRuntimeManager functionRuntimeManager;
    private MembershipManager membershipManager;
    private CompletableFuture<MessageId> completableFuture;
    private Producer producer;
    private TypedMessageBuilder<byte[]> message;
    private ScheduledExecutorService executor;
    private LeaderService leaderService;
    private ErrorNotifier errorNotifier;
    private PulsarClient pulsarClient;

    // Ops on the drain map used in this UT.
    enum DrainOps {
        GetDrainStatus,  // get drain status of a worker
        SetDrainStatus,  // set the status of a worker
        ClearDrainMap    // clear the entire thing
    };

    @BeforeMethod
    public void setup() {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");
        workerConfig.setSchedulerClassName(RoundRobinScheduler.class.getName());
        workerConfig.setAssignmentWriteMaxRetries(0);

        producer = mock(Producer.class);
        completableFuture = spy(new CompletableFuture<>());
        completableFuture.complete(MessageId.earliest);
        //byte[] bytes = any();
        message = mock(TypedMessageBuilder.class);
        when(producer.newMessage()).thenReturn(message);
        when(message.key(anyString())).thenReturn(message);
        when(message.value(any())).thenReturn(message);
        when(message.sendAsync()).thenReturn(completableFuture);

        ProducerBuilder<byte[]> builder = mock(ProducerBuilder.class);
        when(builder.topic(anyString())).thenReturn(builder);
        when(builder.producerName(anyString())).thenReturn(builder);
        when(builder.enableBatching(anyBoolean())).thenReturn(builder);
        when(builder.blockIfQueueFull(anyBoolean())).thenReturn(builder);
        when(builder.compressionType(any(CompressionType.class))).thenReturn(builder);
        when(builder.sendTimeout(anyInt(), any(TimeUnit.class))).thenReturn(builder);
        when(builder.accessMode(any())).thenReturn(builder);

        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(producer));

        pulsarClient = mock(PulsarClient.class);
        when(pulsarClient.newProducer()).thenReturn(builder);

        this.executor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("worker-test"));
        errorNotifier = spy(ErrorNotifier.getDefaultImpl());
        schedulerManager = spy(new SchedulerManager(workerConfig, pulsarClient,
          null, mock(WorkerStatsManager.class), errorNotifier));
        functionRuntimeManager = mock(FunctionRuntimeManager.class);
        functionMetaDataManager = mock(FunctionMetaDataManager.class);
        membershipManager = mock(MembershipManager.class);
        leaderService = mock(LeaderService.class);
        schedulerManager.setFunctionMetaDataManager(functionMetaDataManager);
        schedulerManager.setFunctionRuntimeManager(functionRuntimeManager);
        schedulerManager.setMembershipManager(membershipManager);
        schedulerManager.setLeaderService(leaderService);
    }

    @AfterMethod(alwaysRun = true)
    public void stop() {
        this.executor.shutdownNow();
    }

    @Test
    public void testSchedule() throws Exception {

        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        // single node
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am not leader
        doReturn(false).when(leaderService).isLeader();
        callSchedule();
        verify(producer, times(0)).sendAsync(any());

        // i am leader
        doReturn(true).when(leaderService).isLeader();
        callSchedule();
        List<Invocation> invocations = getMethodInvocationDetails(schedulerManager,
                SchedulerManager.class.getDeclaredMethod("invokeScheduler"));
        Assert.assertEquals(invocations.size(), 1);
        verify(errorNotifier, times(0)).triggerError(any());
    }

    @Test
    public void testNothingNewToSchedule() throws Exception {

        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        // single node
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(leaderService).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 0);
        verify(errorNotifier, times(0)).triggerError(any());
    }

    @Test
    public void testAddingFunctions() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        // single node
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(leaderService).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 1);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));
        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Assignment assignments = Assignment.parseFrom(send);

        log.info("assignments: {}", assignments);
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();
        Assert.assertEquals(assignment2, assignments);

        // make sure we also directly added the assignment to in memory assignment cache in function runtime manager
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2));
    }

    @Test
    public void testDeletingFunctions() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        // simulate function2 got removed
        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1))
                .build();
        functionMetaDataList.add(function1);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        // Delete this assignment
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        //TODO: delete this assignment
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance()), assignment2);

        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        // single node
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(leaderService).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 1);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));
        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];

        // delete assignment message should only have key = full qualified instance id and value = null;
        Assert.assertEquals(0, send.length);

        // make sure we also directly deleted the assignment from the in memory assignment cache in function runtime manager
        verify(functionRuntimeManager, times(1))
                .deleteAssignment(eq(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance())));
    }

    @Test
    public void testScalingUp() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);

        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        // single node
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(leaderService).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 1);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));
        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Assignment assignments = Assignment.parseFrom(send);

        log.info("assignments: {}", assignments);

        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();
        Assert.assertEquals(assignments, assignment2);

        // updating assignments
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance()), assignment2);

        // scale up

        Function.FunctionMetaData function2Scaled = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(3)).setVersion(version)
                .build();
        functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2Scaled);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        Function.Assignment assignment2Scaled1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2Scaled).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2Scaled2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2Scaled).setInstanceId(1).build())
                .build();
        Function.Assignment assignment2Scaled3 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2Scaled).setInstanceId(2).build())
                .build();

        callSchedule();

        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 4);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        Set<Assignment> allAssignments = new HashSet<>();
        invocations.forEach(invocation -> {
            try {
                allAssignments.add(Assignment.parseFrom((byte[]) invocation.getRawArguments()[0]));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(allAssignments.contains(assignment2Scaled1));
        assertTrue(allAssignments.contains(assignment2Scaled2));
        assertTrue(allAssignments.contains(assignment2Scaled3));
    }

    @Test
    public void testScalingDown() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(3)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);

        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        // single node
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(leaderService).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 3);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        for (int i = 0; i < invocations.size(); i++) {
            Invocation invocation = invocations.get(i);
            byte[] send = (byte[]) invocation.getRawArguments()[0];
            Assignment assignment = Assignment.parseFrom(send);
            Assignment expectedAssignment = Function.Assignment.newBuilder()
                    .setWorkerId("worker-1")
                    .setInstance(Function.Instance.newBuilder()
                            .setFunctionMetaData(function2).setInstanceId(i).build())
                    .build();
            Assert.assertEquals(assignment, expectedAssignment);
        }

        Set<Assignment> allAssignments = new HashSet<>();
        invocations.forEach(invocation -> {
            try {
                allAssignments.add(Assignment.parseFrom((byte[]) invocation.getRawArguments()[0]));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });

        Function.Assignment assignment2_1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2_2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(1).build())
                .build();
        Function.Assignment assignment2_3 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(2).build())
                .build();

        assertTrue(allAssignments.contains(assignment2_1));
        assertTrue(allAssignments.contains(assignment2_2));
        assertTrue(allAssignments.contains(assignment2_3));

        // make sure we also directly add the assignment to the in memory assignment cache in function runtime manager
        verify(functionRuntimeManager, times(3)).processAssignment(any());
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2_1));
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2_2));
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2_3));

        // updating assignments
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_1.getInstance()), assignment2_1);
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_2.getInstance()), assignment2_2);
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_3.getInstance()), assignment2_3);

        // scale down

        Function.FunctionMetaData function2Scaled = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2Scaled);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        Function.Assignment assignment2Scaled = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2Scaled).setInstanceId(0).build())
                .build();

        callSchedule();

        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 6);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        Set<Assignment> allAssignments2 = new HashSet<>();
        invocations.forEach(invocation -> {
            try {
                allAssignments2.add(Assignment.parseFrom((byte[]) invocation.getRawArguments()[0]));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(allAssignments2.contains(assignment2Scaled));

        // make sure we also directly removed the assignment from the in memory assignment cache in function runtime manager
        verify(functionRuntimeManager, times(2)).deleteAssignment(anyString());
        verify(functionRuntimeManager, times(1))
                .deleteAssignment(eq(FunctionCommon.getFullyQualifiedInstanceId(assignment2_2.getInstance())));
        verify(functionRuntimeManager, times(1))
                .deleteAssignment(eq(FunctionCommon.getFullyQualifiedInstanceId(assignment2_2.getInstance())));

        verify(functionRuntimeManager, times(4)).processAssignment(any());
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2Scaled));
    }

    @Test
    public void testHeartbeatFunction() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        final long version = 5;
        final String workerId1 = "host-workerId-1";
        final String workerId2 = "host-workerId-2";
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName(workerId1)
                        .setNamespace(SchedulerManager.HEARTBEAT_NAMESPACE)
                        .setTenant(SchedulerManager.HEARTBEAT_TENANT).setParallelism(1))
                .setVersion(version).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName(workerId2)
                        .setNamespace(SchedulerManager.HEARTBEAT_NAMESPACE)
                        .setTenant(SchedulerManager.HEARTBEAT_TENANT).setParallelism(1))
                .setVersion(version).build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();

        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of(workerId1, "workerHostname-1", 5000));
        workerInfoList.add(WorkerInfo.of(workerId2, "workerHostname-1", 6000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(leaderService).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 2);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));
        invocations.forEach(invocation -> {
            try {
                Assignment assignment = Assignment.parseFrom((byte[]) invocation.getRawArguments()[0]);
                String functionName = assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName();
                String assignedWorkerId = assignment.getWorkerId();
                Assert.assertEquals(functionName, assignedWorkerId);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testUpdate() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/foo/bar1"))
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/foo/bar1"))
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(3)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);

        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        // single node
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(leaderService).isLeader();

        callSchedule();

        Function.Assignment assignment2_1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2_2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(1).build())
                .build();
        Function.Assignment assignment2_3 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(2).build())
                .build();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 3);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        Set<Assignment> allAssignments = new HashSet<>();
        invocations.forEach(invocation -> {
            try {
                allAssignments.add(Assignment.parseFrom((byte[]) invocation.getRawArguments()[0]));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(allAssignments.size(), 3);
        assertTrue(allAssignments.contains(assignment2_1));
        assertTrue(allAssignments.contains(assignment2_2));
        assertTrue(allAssignments.contains(assignment2_3));

        // make sure we also directly add the assignment to the in memory assignment cache in function runtime manager
        verify(functionRuntimeManager, times(3)).processAssignment(any());
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2_1));
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2_2));
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2_3));

        // updating assignments
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_1.getInstance()), assignment2_1);
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_2.getInstance()), assignment2_2);
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_3.getInstance()), assignment2_3);

        // update field

        Function.FunctionMetaData function2Updated = Function.FunctionMetaData.newBuilder()
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/foo/bar2"))
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(3)).setVersion(version)
                .build();
        functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2Updated);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        Function.Assignment assignment2Updated1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2Updated).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2Updated2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2Updated).setInstanceId(1).build())
                .build();
        Function.Assignment assignment2Updated3 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2Updated).setInstanceId(2).build())
                .build();

        callSchedule();

        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 6);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        Set<Assignment> allAssignments2 = new HashSet<>();
        invocations.forEach(invocation -> {
            try {
                allAssignments2.add(Assignment.parseFrom((byte[]) invocation.getRawArguments()[0]));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(allAssignments2.contains(assignment2Updated1));
        assertTrue(allAssignments2.contains(assignment2Updated2));
        assertTrue(allAssignments2.contains(assignment2Updated3));

        // make sure we also directly updated the assignment to the in memory assignment cache in function runtime manager
        verify(functionRuntimeManager, times(6)).processAssignment(any());
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2Updated1));
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2Updated2));
        verify(functionRuntimeManager, times(1)).processAssignment(eq(assignment2Updated3));
    }

    @Test
    public void testAssignmentWorkerDoesNotExist() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        // set assignment to worker that doesn't exist / died
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-2")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        currentAssignments.put("worker-1", assignmentEntry1);

        Map<String, Function.Assignment> assignmentEntry2 = new HashMap<>();
        assignmentEntry2.put(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance()), assignment2);
        currentAssignments.put("worker-2", assignmentEntry2);

        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        // single node
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(leaderService).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 0);
    }

    @Test
    public void testDrain() throws Exception {

        final int numWorkers = 4;

        List<WorkerInfo> workerInfoList = new LinkedList<>();
        for (int ix = 0; ix < numWorkers; ix++) {
            String workerId = "worker-" + ix;
            String workerHostName = "workerHostname-" + ix;
            workerInfoList.add(WorkerInfo.of(workerId, workerHostName, 5000));
        }
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // Set up multiple functions and assignments, so that there are more functions than workers.
        final int numFunctions = numWorkers * 5;
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        for (int ix = 0; ix < numFunctions; ix++) {
            String funcName = "func-" + ix;
            Function.FunctionMetaData func = Function.FunctionMetaData.newBuilder()
                    .setFunctionDetails(Function.FunctionDetails.newBuilder().setName(funcName)
                            .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                    .build();
            functionMetaDataList.add(func);
        }
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments, round-robining the functions among the workers
        int workerIndex = 0;
        List<Function.Assignment> assignmentList = new LinkedList<>();
        for (val func : functionMetaDataList) {
            String workerId = "worker-" + workerIndex;
            Function.Assignment ass = Function.Assignment.newBuilder()
                    .setWorkerId(workerId)
                    .setInstance(Function.Instance.newBuilder()
                            .setFunctionMetaData(func)
                            .setInstanceId(0)
                            .build())
                    .build();
            assignmentList.add(ass);
            workerIndex = (workerIndex + 1) % numWorkers;
        }

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();

        final String workerIdToDrain = "worker-0";
        int numAssignmentsOnDrainedWorker = 0;

        workerIndex = 0;
        for (val ass : assignmentList) {
            String workerId = "worker-" + workerIndex;
            Map<String, Function.Assignment> assignmentEntry = currentAssignments.get(workerId);
            if (assignmentEntry == null) {
                assignmentEntry = new HashMap<>();
            }
            assignmentEntry.put(FunctionCommon.getFullyQualifiedInstanceId(ass.getInstance()), ass);
            currentAssignments.put(workerId, assignmentEntry);
            if (workerId.compareTo(workerIdToDrain) == 0) {
                numAssignmentsOnDrainedWorker++;
            }
            workerIndex = (workerIndex + 1) % numWorkers;
        }
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        // i am not leader
        doReturn(false).when(leaderService).isLeader();
        val assignmentsMovedNonLeader = callDrain(workerIdToDrain);
        verify(producer, times(0)).sendAsync(any());
        Assert.assertTrue(assignmentsMovedNonLeader == null);

        // i am leader
        doReturn(true).when(leaderService).isLeader();
        val assignmentsMovedLeader = callDrain(workerIdToDrain);
        List<Invocation> invocations = getMethodInvocationDetails(schedulerManager,
                SchedulerManager.class.getDeclaredMethod("invokeDrain", String.class));
        Assert.assertEquals(invocations.size(), 1);
        verify(errorNotifier, times(0)).triggerError(any());
        Assert.assertFalse(assignmentsMovedLeader.isEmpty());
        // New assignment must contain all the functions.
        Assert.assertEquals(assignmentsMovedLeader.size(), numAssignmentsOnDrainedWorker);
        // Shouldn't see the drained worker in any of the new assignments.
        assignmentsMovedLeader.forEach(ass ->
                Assert.assertTrue(ass.getWorkerId().compareTo(workerIdToDrain) != 0));

        // Get the status of the drain op; since it's a get, the passed-in drain op status is a don't care.
        val dStatus = callGetDrainStatus(workerIdToDrain,
                DrainOps.GetDrainStatus, SchedulerManager.DrainOpStatus.DrainNotInProgress);
        Assert.assertTrue(dStatus.status == LongRunningProcessStatus.Status.SUCCESS);
        Assert.assertTrue(dStatus.lastError.isEmpty());
    }

    @Test
    public void testGetDrainStatus() throws Exception {
        // Clear the drain status map in the SchedulerManager; all other parameters are don't care for a clear.
        callGetDrainStatus(null, DrainOps.ClearDrainMap,
                SchedulerManager.DrainOpStatus.DrainCompleted);

        // Set up drain status for some fake workers.
        callGetDrainStatus("worker-1", DrainOps.SetDrainStatus,
                SchedulerManager.DrainOpStatus.DrainCompleted);
        callGetDrainStatus("worker-2", DrainOps.SetDrainStatus,
                SchedulerManager.DrainOpStatus.DrainInProgress);
        callGetDrainStatus("worker-3", DrainOps.SetDrainStatus,
                SchedulerManager.DrainOpStatus.DrainNotInProgress);

        // Get status; the status passed in a a don't care.
        LongRunningProcessStatus dStatus;
        dStatus = callGetDrainStatus("worker-0", DrainOps.GetDrainStatus,
                SchedulerManager.DrainOpStatus.DrainNotInProgress);
        Assert.assertTrue(dStatus.status == LongRunningProcessStatus.Status.ERROR);
        Assert.assertTrue(dStatus.lastError.matches("(.)+(not found)(.)+"));

        dStatus = callGetDrainStatus("worker-1", DrainOps.GetDrainStatus,
                SchedulerManager.DrainOpStatus.DrainNotInProgress);
        Assert.assertTrue(dStatus.status == LongRunningProcessStatus.Status.SUCCESS);
        Assert.assertTrue(dStatus.lastError.isEmpty());

        dStatus = callGetDrainStatus("worker-2", DrainOps.GetDrainStatus,
                SchedulerManager.DrainOpStatus.DrainNotInProgress);
        Assert.assertTrue(dStatus.status == LongRunningProcessStatus.Status.RUNNING);
        Assert.assertTrue(dStatus.lastError.isEmpty());

        dStatus = callGetDrainStatus("worker-3", DrainOps.GetDrainStatus,
                SchedulerManager.DrainOpStatus.DrainNotInProgress);
        Assert.assertTrue(dStatus.status == LongRunningProcessStatus.Status.NOT_RUN);
        Assert.assertTrue(dStatus.lastError.isEmpty());
    }

    @Test
    public void testDrainExceptions() throws Exception {

        // i am leader
        doReturn(true).when(leaderService).isLeader();

        // TooFewWorkersException when drain called with a single worker
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        final String workerIdToDrain = "worker-0";
        workerInfoList.add(WorkerInfo.of(workerIdToDrain, "worker-Hostname-0", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();
        Assert.expectThrows(SchedulerManager.TooFewWorkersException.class, () -> callDrain(workerIdToDrain));

        // WorkerNotRemovedAfterPriorDrainException when drain is called more than once on same worker.
        final int numWorkers = 5;
        for (int ix = 1; ix < numWorkers; ix++) {
            String workerId = "worker-" + ix;
            String workerHostName = "workerHostname-" + ix;
            workerInfoList.add(WorkerInfo.of(workerId, workerHostName, 5000));
        }
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();
        callDrain(workerIdToDrain);
        Assert.expectThrows(SchedulerManager.WorkerNotRemovedAfterPriorDrainException.class,
                () -> callDrain(workerIdToDrain));

        // Ask to drain a worker which isn't present.
        String unknownWorkerId = "UnknownWorker";
        Assert.expectThrows(SchedulerManager.UnknownWorkerException.class, () -> callDrain(unknownWorkerId));
    }

    @Test
    public void testUpdateWorkerDrainMap() throws Exception {
        final int numWorkersInDrainMap = 5;
        String workerId;

        // Set up drain status for some of those workers.
        SchedulerManager.DrainOpStatus drainOp;
        for (int ix = 0; ix < numWorkersInDrainMap; ix++) {
            workerId = "worker-" + ix;
            drainOp = SchedulerManager.DrainOpStatus.DrainCompleted;
            if (ix % 2 == 0) {
                drainOp = SchedulerManager.DrainOpStatus.DrainInProgress;
            }
            callGetDrainStatus(workerId, DrainOps.SetDrainStatus, drainOp);
        }
        val oldDrainMap = schedulerManager.getDrainOpsStatusMap();

        final int numWorkersInCurrentMembership = 3;
        List<WorkerInfo> workerInfoList = new LinkedList<>();
        final String workerHostName = "workerHostName";
        final int workerPort = 5000;
        for (int ix = 0; ix < numWorkersInCurrentMembership; ix++) {
            workerId = "worker-" + ix;
            workerInfoList.add(WorkerInfo.of(workerId, workerHostName, workerPort));
        }
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        val numWorkersCleanedUp = schedulerManager.updateWorkerDrainMap();
        Assert.assertEquals(numWorkersCleanedUp, numWorkersInDrainMap - numWorkersInCurrentMembership);

        val newDrainMap = schedulerManager.getDrainOpsStatusMap();
        for (val worker : newDrainMap.keySet()) {
            Assert.assertTrue(oldDrainMap.get(worker) != null);
            WorkerInfo matchedWorker = workerInfoList.stream()
                    .filter(winfo -> worker.equals(winfo.getWorkerId()))
                    .findAny()
                    .orElse(null);
            Assert.assertTrue(matchedWorker != null);
        }
    }

    private void callSchedule() throws InterruptedException,
            TimeoutException, ExecutionException, WorkerUtils.NotLeaderAnymore {

        if (leaderService.isLeader()) {
            Producer<byte[]> exclusiveProducer = schedulerManager.acquireExclusiveWrite(() -> true);
            schedulerManager.initialize(exclusiveProducer);
        }
        Future<?> complete = schedulerManager.schedule();

        complete.get(30, TimeUnit.SECONDS);
    }

    private List<Assignment> callDrain(String workerId) throws InterruptedException,
            TimeoutException, ExecutionException, WorkerUtils.NotLeaderAnymore {

        schedulerManager.clearAssignmentsMovedInLastDrain();

        if (leaderService.isLeader()) {
            Producer<byte[]> exclusiveProducer = schedulerManager.acquireExclusiveWrite(() -> true);
            schedulerManager.initialize(exclusiveProducer);
        }
        Future<?> complete = schedulerManager.drainIfNotInProgress(workerId);
        complete.get(30, TimeUnit.SECONDS);

        return schedulerManager.getAssignmentsMovedInLastDrain();
    }

    private LongRunningProcessStatus callGetDrainStatus(String workerId, DrainOps op,
                                                        SchedulerManager.DrainOpStatus drainOpStatus) {

        switch (op) {
            default:
                Assert.fail("Unexpected drain operation");
            case GetDrainStatus:
                return schedulerManager.getDrainStatus(workerId);
            case SetDrainStatus:
                schedulerManager.setDrainOpsStatus(workerId, drainOpStatus);
                return LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.SUCCESS);
            case ClearDrainMap:
                schedulerManager.clearDrainOpsStatus();
                return LongRunningProcessStatus.forStatus(LongRunningProcessStatus.Status.SUCCESS);
        }
    }

    private List<Invocation> getMethodInvocationDetails(Object o, Method method) throws NoSuchMethodException {
        List<Invocation> ret = new LinkedList<>();
        for (Invocation entry : Mockito.mockingDetails(o).getInvocations()) {
            if (entry.getMethod().getName().equals(method.getName())) {
                ret.add(entry);
            }
        }
        return ret;
    }
}
