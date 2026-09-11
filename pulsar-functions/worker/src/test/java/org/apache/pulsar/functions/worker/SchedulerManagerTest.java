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

import static org.mockito.ArgumentMatchers.argThat;
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
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.CustomLog;
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
import org.apache.pulsar.functions.proto.Assignment;
import org.apache.pulsar.functions.proto.FunctionMetaData;
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

@CustomLog
public class SchedulerManagerTest {

    private SchedulerManager schedulerManager;
    private FunctionMetaDataManager functionMetaDataManager;
    private FunctionRuntimeManager functionRuntimeManager;
    private MembershipManager membershipManager;
    private CompletableFuture<MessageId> completableFuture;
    @SuppressWarnings("rawtypes")
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

    private static FunctionMetaData createFunctionMetaData(String tenant, String namespace, String name,
                                                            int parallelism, long version) {
        FunctionMetaData fmd = new FunctionMetaData();
        fmd.setFunctionDetails().setName(name).setNamespace(namespace).setTenant(tenant).setParallelism(parallelism);
        fmd.setVersion(version);
        return fmd;
    }

    private static Assignment createAssignment(String workerId, FunctionMetaData function, int instanceId) {
        Assignment assignment = new Assignment();
        assignment.setWorkerId(workerId);
        assignment.setInstance().setFunctionMetaData().copyFrom(function);
        assignment.getInstance().setInstanceId(instanceId);
        return assignment;
    }

    private static Assignment parseAssignment(byte[] data) {
        Assignment assignment = new Assignment();
        assignment.parseFrom(data);
        return assignment;
    }

    @BeforeMethod
    @SuppressWarnings("unchecked")
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
        schedulerManager.close();
        this.executor.shutdownNow();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSchedule() throws Exception {

        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        FunctionMetaData function1 = createFunctionMetaData("tenant-1", "namespace-1", "func-1", 1, version);
        functionMetaDataList.add(function1);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Assignment assignment1 = createAssignment("worker-1", function1, 0);

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();
        Map<String, Assignment> assignmentEntry1 = new HashMap<>();
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

        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        FunctionMetaData function1 = createFunctionMetaData("tenant-1", "namespace-1", "func-1", 1, version);
        functionMetaDataList.add(function1);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Assignment assignment1 = createAssignment("worker-1", function1, 0);

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();
        Map<String, Assignment> assignmentEntry1 = new HashMap<>();
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
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        FunctionMetaData function1 = createFunctionMetaData("tenant-1", "namespace-1", "func-1", 1, version);
        FunctionMetaData function2 = createFunctionMetaData("tenant-1", "namespace-1", "func-2", 1, version);
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Assignment assignment1 = createAssignment("worker-1", function1, 0);

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();
        Map<String, Assignment> assignmentEntry1 = new HashMap<>();
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
        Assignment assignments = parseAssignment(send);

        log.info().attr("assignments", assignments).log("assignments");
        Assignment assignment2 = createAssignment("worker-1", function2, 0);
        Assert.assertEquals(assignment2.toByteArray(), assignments.toByteArray());

        // make sure we also directly added the assignment to in memory assignment cache in function runtime manager
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment2.toByteArray())));
    }

    @Test
    public void testDeletingFunctions() throws Exception {
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        FunctionMetaData function1 = createFunctionMetaData("tenant-1", "namespace-1", "func-1", 1, version);

        // simulate function2 got removed
        FunctionMetaData function2 = new FunctionMetaData();
        function2.setFunctionDetails().setName("func-2")
                .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1);
        functionMetaDataList.add(function1);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Assignment assignment1 = createAssignment("worker-1", function1, 0);

        // Delete this assignment
        Assignment assignment2 = createAssignment("worker-1", function2, 0);

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();
        Map<String, Assignment> assignmentEntry1 = new HashMap<>();
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

        // make sure we also directly deleted the assignment from the in memory assignment cache in
        // function runtime manager
        verify(functionRuntimeManager, times(1))
                .deleteAssignment(eq(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance())));
    }

    @Test
    public void testScalingUp() throws Exception {
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        FunctionMetaData function1 = createFunctionMetaData("tenant-1", "namespace-1", "func-1", 1, version);
        FunctionMetaData function2 = createFunctionMetaData("tenant-1", "namespace-1", "func-2", 1, version);
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Assignment assignment1 = createAssignment("worker-1", function1, 0);

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();
        Map<String, Assignment> assignmentEntry1 = new HashMap<>();
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
        Assignment assignments = parseAssignment(send);

        log.info().attr("assignments", assignments).log("assignments");

        Assignment assignment2 = createAssignment("worker-1", function2, 0);
        Assert.assertEquals(assignments.toByteArray(), assignment2.toByteArray());

        // updating assignments
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance()), assignment2);

        // scale up

        FunctionMetaData function2Scaled = createFunctionMetaData("tenant-1", "namespace-1", "func-2", 3, version);
        functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2Scaled);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        Assignment assignment2Scaled1 = createAssignment("worker-1", function2Scaled, 0);
        Assignment assignment2Scaled2 = createAssignment("worker-1", function2Scaled, 1);
        Assignment assignment2Scaled3 = createAssignment("worker-1", function2Scaled, 2);

        callSchedule();

        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 4);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        List<byte[]> allAssignmentBytesScaled = new ArrayList<>();
        invocations.forEach(invocation -> {
            allAssignmentBytesScaled.add(parseAssignment((byte[]) invocation.getRawArguments()[0]).toByteArray());
        });

        assertTrue(allAssignmentBytesScaled.stream().anyMatch(b -> Arrays.equals(b, assignment2Scaled1.toByteArray())));
        assertTrue(allAssignmentBytesScaled.stream().anyMatch(b -> Arrays.equals(b, assignment2Scaled2.toByteArray())));
        assertTrue(allAssignmentBytesScaled.stream().anyMatch(b -> Arrays.equals(b, assignment2Scaled3.toByteArray())));
    }

    @Test
    public void testScalingDown() throws Exception {
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        FunctionMetaData function1 = createFunctionMetaData("tenant-1", "namespace-1", "func-1", 1, version);
        FunctionMetaData function2 = createFunctionMetaData("tenant-1", "namespace-1", "func-2", 3, version);
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Assignment assignment1 = createAssignment("worker-1", function1, 0);

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();
        Map<String, Assignment> assignmentEntry1 = new HashMap<>();
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

        List<byte[]> allAssignmentBytes = new ArrayList<>();
        invocations.forEach(invocation -> {
            allAssignmentBytes.add(parseAssignment((byte[]) invocation.getRawArguments()[0]).toByteArray());
        });

        Assignment assignment21 = createAssignment("worker-1", function2, 0);
        Assignment assignment22 = createAssignment("worker-1", function2, 1);
        Assignment assignment23 = createAssignment("worker-1", function2, 2);

        assertTrue(allAssignmentBytes.stream().anyMatch(b -> Arrays.equals(b, assignment21.toByteArray())));
        assertTrue(allAssignmentBytes.stream().anyMatch(b -> Arrays.equals(b, assignment22.toByteArray())));
        assertTrue(allAssignmentBytes.stream().anyMatch(b -> Arrays.equals(b, assignment23.toByteArray())));

        // make sure we also directly add the assignment to the in memory assignment cache in function runtime manager
        verify(functionRuntimeManager, times(3)).processAssignment(any());
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment21.toByteArray())));
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment22.toByteArray())));
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment23.toByteArray())));

        // updating assignments
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment21.getInstance()), assignment21);
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment22.getInstance()), assignment22);
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment23.getInstance()), assignment23);

        // scale down

        FunctionMetaData function2Scaled = createFunctionMetaData("tenant-1", "namespace-1", "func-2", 1, version);
        functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2Scaled);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        Assignment assignment2Scaled = createAssignment("worker-1", function2Scaled, 0);

        callSchedule();

        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 6);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        List<byte[]> allAssignmentBytes2 = new ArrayList<>();
        invocations.forEach(invocation -> {
            allAssignmentBytes2.add(parseAssignment((byte[]) invocation.getRawArguments()[0]).toByteArray());
        });

        assertTrue(allAssignmentBytes2.stream().anyMatch(b -> Arrays.equals(b, assignment2Scaled.toByteArray())));

        // make sure we also directly removed the assignment from the in memory assignment cache in
        // function runtime manager
        verify(functionRuntimeManager, times(2)).deleteAssignment(anyString());
        verify(functionRuntimeManager, times(1))
                .deleteAssignment(eq(FunctionCommon.getFullyQualifiedInstanceId(assignment22.getInstance())));
        verify(functionRuntimeManager, times(1))
                .deleteAssignment(eq(FunctionCommon.getFullyQualifiedInstanceId(assignment22.getInstance())));

        verify(functionRuntimeManager, times(4)).processAssignment(any());
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment2Scaled.toByteArray())));
    }

    @Test
    public void testHeartbeatFunction() throws Exception {
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        final long version = 5;
        final String workerId1 = "host-workerId-1";
        final String workerId2 = "host-workerId-2";
        FunctionMetaData function1 = new FunctionMetaData();
        function1.setFunctionDetails().setName(workerId1)
                .setNamespace(SchedulerManager.HEARTBEAT_NAMESPACE)
                .setTenant(SchedulerManager.HEARTBEAT_TENANT).setParallelism(1);
        function1.setVersion(version);

        FunctionMetaData function2 = new FunctionMetaData();
        function2.setFunctionDetails().setName(workerId2)
                .setNamespace(SchedulerManager.HEARTBEAT_NAMESPACE)
                .setTenant(SchedulerManager.HEARTBEAT_TENANT).setParallelism(1);
        function2.setVersion(version);
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();
        Map<String, Assignment> assignmentEntry1 = new HashMap<>();

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
            Assignment assignment = parseAssignment((byte[]) invocation.getRawArguments()[0]);
            String functionName = assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName();
            String assignedWorkerId = assignment.getWorkerId();
            Assert.assertEquals(functionName, assignedWorkerId);
        });
    }

    @Test
    public void testUpdate() throws Exception {
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        FunctionMetaData function1 = new FunctionMetaData();
        function1.setPackageLocation().setPackagePath("/foo/bar1");
        function1.setFunctionDetails().setName("func-1")
                .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1);
        function1.setVersion(version);

        FunctionMetaData function2 = new FunctionMetaData();
        function2.setPackageLocation().setPackagePath("/foo/bar1");
        function2.setFunctionDetails().setName("func-2")
                .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(3);
        function2.setVersion(version);
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Assignment assignment1 = createAssignment("worker-1", function1, 0);

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();
        Map<String, Assignment> assignmentEntry1 = new HashMap<>();
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

        Assignment assignment21 = createAssignment("worker-1", function2, 0);
        Assignment assignment22 = createAssignment("worker-1", function2, 1);
        Assignment assignment23 = createAssignment("worker-1", function2, 2);

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 3);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        List<byte[]> allAssignmentBytes = new ArrayList<>();
        invocations.forEach(invocation -> {
            allAssignmentBytes.add(parseAssignment((byte[]) invocation.getRawArguments()[0]).toByteArray());
        });

        assertEquals(allAssignmentBytes.size(), 3);
        assertTrue(allAssignmentBytes.stream().anyMatch(b -> Arrays.equals(b, assignment21.toByteArray())));
        assertTrue(allAssignmentBytes.stream().anyMatch(b -> Arrays.equals(b, assignment22.toByteArray())));
        assertTrue(allAssignmentBytes.stream().anyMatch(b -> Arrays.equals(b, assignment23.toByteArray())));

        // make sure we also directly add the assignment to the in memory assignment cache in function runtime manager
        verify(functionRuntimeManager, times(3)).processAssignment(any());
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment21.toByteArray())));
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment22.toByteArray())));
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment23.toByteArray())));

        // updating assignments
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment21.getInstance()), assignment21);
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment22.getInstance()), assignment22);
        currentAssignments.get("worker-1")
                .put(FunctionCommon.getFullyQualifiedInstanceId(assignment23.getInstance()), assignment23);

        // update field

        FunctionMetaData function2Updated = new FunctionMetaData();
        function2Updated.setPackageLocation().setPackagePath("/foo/bar2");
        function2Updated.setFunctionDetails().setName("func-2")
                .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(3);
        function2Updated.setVersion(version);
        functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2Updated);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        Assignment assignment2Updated1 = createAssignment("worker-1", function2Updated, 0);
        Assignment assignment2Updated2 = createAssignment("worker-1", function2Updated, 1);
        Assignment assignment2Updated3 = createAssignment("worker-1", function2Updated, 2);

        callSchedule();

        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("send"));
        Assert.assertEquals(invocations.size(), 6);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        List<byte[]> allAssignmentBytes3 = new ArrayList<>();
        invocations.forEach(invocation -> {
            allAssignmentBytes3.add(parseAssignment((byte[]) invocation.getRawArguments()[0]).toByteArray());
        });

        assertTrue(allAssignmentBytes3.stream().anyMatch(b -> Arrays.equals(b, assignment2Updated1.toByteArray())));
        assertTrue(allAssignmentBytes3.stream().anyMatch(b -> Arrays.equals(b, assignment2Updated2.toByteArray())));
        assertTrue(allAssignmentBytes3.stream().anyMatch(b -> Arrays.equals(b, assignment2Updated3.toByteArray())));

        // make sure we also directly updated the assignment to the in memory assignment cache in
        // function runtime manager
        verify(functionRuntimeManager, times(6)).processAssignment(any());
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment2Updated1.toByteArray())));
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment2Updated2.toByteArray())));
        verify(functionRuntimeManager, times(1)).processAssignment(
                argThat(a -> Arrays.equals(a.toByteArray(), assignment2Updated3.toByteArray())));
    }

    @Test
    public void testAssignmentWorkerDoesNotExist() throws Exception {
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        FunctionMetaData function1 = createFunctionMetaData("tenant-1", "namespace-1", "func-1", 1, version);
        FunctionMetaData function2 = createFunctionMetaData("tenant-1", "namespace-1", "func-2", 1, version);
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments
        Assignment assignment1 = createAssignment("worker-1", function1, 0);

        // set assignment to worker that doesn't exist / died
        Assignment assignment2 = createAssignment("worker-2", function2, 0);

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();
        Map<String, Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        currentAssignments.put("worker-1", assignmentEntry1);

        Map<String, Assignment> assignmentEntry2 = new HashMap<>();
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
    @SuppressWarnings("unchecked")
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
        List<FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        for (int ix = 0; ix < numFunctions; ix++) {
            String funcName = "func-" + ix;
            FunctionMetaData func = createFunctionMetaData("tenant-1", "namespace-1", funcName, 1, version);
            functionMetaDataList.add(func);
        }
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        doReturn(factory).when(functionRuntimeManager).getRuntimeFactory();

        // set assignments, round-robining the functions among the workers
        int workerIndex = 0;
        List<Assignment> assignmentList = new LinkedList<>();
        for (val func : functionMetaDataList) {
            String workerId = "worker-" + workerIndex;
            Assignment ass = createAssignment(workerId, func, 0);
            assignmentList.add(ass);
            workerIndex = (workerIndex + 1) % numWorkers;
        }

        Map<String, Map<String, Assignment>> currentAssignments = new HashMap<>();

        final String workerIdToDrain = "worker-0";
        int numAssignmentsOnDrainedWorker = 0;

        workerIndex = 0;
        for (val ass : assignmentList) {
            String workerId = "worker-" + workerIndex;
            Map<String, Assignment> assignmentEntry = currentAssignments.get(workerId);
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
                return null;
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
