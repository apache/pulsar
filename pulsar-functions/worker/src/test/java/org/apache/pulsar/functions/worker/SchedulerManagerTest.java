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

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.prometheus.client.CollectorRegistry;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
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

    @BeforeMethod
    public void setup() {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getThreadLocal().convertValue(
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
        when(builder.enableBatching(anyBoolean())).thenReturn(builder);
        when(builder.blockIfQueueFull(anyBoolean())).thenReturn(builder);
        when(builder.compressionType(any(CompressionType.class))).thenReturn(builder);
        when(builder.sendTimeout(anyInt(), any(TimeUnit.class))).thenReturn(builder);

        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(producer));

        PulsarClient pulsarClient = mock(PulsarClient.class);
        when(pulsarClient.newProducer()).thenReturn(builder);

        this.executor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("worker-test"));
        schedulerManager = spy(new SchedulerManager(workerConfig, pulsarClient, null, executor));
        functionRuntimeManager = mock(FunctionRuntimeManager.class);
        functionMetaDataManager = mock(FunctionMetaDataManager.class);
        membershipManager = mock(MembershipManager.class);
        schedulerManager.setFunctionMetaDataManager(functionMetaDataManager);
        schedulerManager.setFunctionRuntimeManager(functionRuntimeManager);
        schedulerManager.setMembershipManager(membershipManager);
    }

    @AfterMethod
    public void stop() {
        this.executor.shutdown();
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

        ThreadRuntimeFactory factory = new ThreadRuntimeFactory("dummy", null, "dummy", new ClearTextSecretsProvider(), new CollectorRegistry(), null);
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
        doReturn(false).when(membershipManager).isLeader();
        callSchedule();
        verify(producer, times(0)).sendAsync(any());

        // i am leader
        doReturn(true).when(membershipManager).isLeader();
        callSchedule();
        List<Invocation> invocations = getMethodInvocationDetails(schedulerManager,
                SchedulerManager.class.getMethod("invokeScheduler"));
        Assert.assertEquals(invocations.size(), 1);
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

        ThreadRuntimeFactory factory = new ThreadRuntimeFactory("dummy", null, "dummy", new ClearTextSecretsProvider(), new CollectorRegistry(), null);
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
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
        Assert.assertEquals(invocations.size(), 0);
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

        ThreadRuntimeFactory factory = new ThreadRuntimeFactory("dummy", null, "dummy", new ClearTextSecretsProvider(), new CollectorRegistry(), null);
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
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
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

        ThreadRuntimeFactory factory = new ThreadRuntimeFactory("dummy", null, "dummy", new ClearTextSecretsProvider(), new CollectorRegistry(), null);
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
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
        Assert.assertEquals(invocations.size(), 1);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));
        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Assignment assignments = Assignment.parseFrom(send);

        log.info("assignments: {}", assignments);

        Assert.assertEquals(0, send.length);
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

        ThreadRuntimeFactory factory = new ThreadRuntimeFactory("dummy", null, "dummy", new ClearTextSecretsProvider
                (), new CollectorRegistry(), null);
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
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
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
        currentAssignments.get("worker-1").put(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance()), assignment2);

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

        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
        Assert.assertEquals(invocations.size(), 4);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        Set<Assignment> allAssignments = Sets.newHashSet();
        invocations.forEach(invocation -> {
            try {
                allAssignments.add(Assignment.parseFrom((byte[])invocation.getRawArguments()[0]));
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

        ThreadRuntimeFactory factory = new ThreadRuntimeFactory("dummy", null, "dummy", new ClearTextSecretsProvider(), new CollectorRegistry(), null);
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
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
        Assert.assertEquals(invocations.size(), 3);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));
        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Assignment assignments = Assignment.parseFrom(send);

        log.info("assignments: {}", assignments);

        Set<Assignment> allAssignments = Sets.newHashSet();
        invocations.forEach(invocation -> {
            try {
                allAssignments.add(Assignment.parseFrom((byte[])invocation.getRawArguments()[0]));
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

        // updating assignments
        currentAssignments.get("worker-1").put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_1.getInstance()), assignment2_1);
        currentAssignments.get("worker-1").put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_2.getInstance()), assignment2_2);
        currentAssignments.get("worker-1").put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_3.getInstance()), assignment2_3);

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

        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
        Assert.assertEquals(invocations.size(), 6);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        Set<Assignment> allAssignments2 = Sets.newHashSet();
        invocations.forEach(invocation -> {
            try {
                allAssignments2.add(Assignment.parseFrom((byte[])invocation.getRawArguments()[0]));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(allAssignments2.contains(assignment2Scaled));
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

        ThreadRuntimeFactory factory = new ThreadRuntimeFactory("dummy", null, "dummy", new ClearTextSecretsProvider(), new CollectorRegistry(), null);
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
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
        Assert.assertEquals(invocations.size(), 2);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));
        invocations.forEach(invocation -> {
            try {
                Assignment assignment = Assignment.parseFrom((byte[])invocation.getRawArguments()[0]);
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

        ThreadRuntimeFactory factory = new ThreadRuntimeFactory("dummy", null, "dummy", new ClearTextSecretsProvider(), new CollectorRegistry(), null);
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
        doReturn(true).when(membershipManager).isLeader();

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

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
        Assert.assertEquals(invocations.size(), 3);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        Set<Assignment> allAssignments = Sets.newHashSet();
        invocations.forEach(invocation -> {
            try {
                allAssignments.add(Assignment.parseFrom((byte[])invocation.getRawArguments()[0]));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(allAssignments.size(), 3);
        assertTrue(allAssignments.contains(assignment2_1));
        assertTrue(allAssignments.contains(assignment2_2));
        assertTrue(allAssignments.contains(assignment2_3));

        // updating assignments
        currentAssignments.get("worker-1").put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_1.getInstance()), assignment2_1);
        currentAssignments.get("worker-1").put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_2.getInstance()), assignment2_2);
        currentAssignments.get("worker-1").put(FunctionCommon.getFullyQualifiedInstanceId(assignment2_3.getInstance()), assignment2_3);

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

        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
        Assert.assertEquals(invocations.size(), 6);
        invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("value",
                Object.class));

        Set<Assignment> allAssignments2 = Sets.newHashSet();
        invocations.forEach(invocation -> {
            try {
                allAssignments2.add(Assignment.parseFrom((byte[])invocation.getRawArguments()[0]));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(allAssignments2.contains(assignment2Updated1));
        assertTrue(allAssignments2.contains(assignment2Updated2));
        assertTrue(allAssignments2.contains(assignment2Updated3));
    }

    @Test
    public void testAssignmentWorkerDoesNotExist() throws InterruptedException, NoSuchMethodException, TimeoutException, ExecutionException, InvalidProtocolBufferException {
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

        ThreadRuntimeFactory factory = new ThreadRuntimeFactory("dummy", null, "dummy", new ClearTextSecretsProvider(), new CollectorRegistry(), null);
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
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(message, TypedMessageBuilder.class.getMethod("sendAsync"));
        Assert.assertEquals(invocations.size(), 0);
    }

    private void callSchedule() throws NoSuchMethodException, InterruptedException,
            TimeoutException, ExecutionException {
        Future<?> complete = schedulerManager.schedule();

        complete.get(30, TimeUnit.SECONDS);
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
