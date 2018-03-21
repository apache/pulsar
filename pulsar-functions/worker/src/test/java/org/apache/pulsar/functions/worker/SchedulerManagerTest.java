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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Request;
import org.apache.pulsar.functions.worker.scheduler.RoundRobinScheduler;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class SchedulerManagerTest {

    private SchedulerManager schedulerManager;
    private FunctionMetaDataManager functionMetaDataManager;
    private FunctionRuntimeManager functionRuntimeManager;
    private MembershipManager membershipManager;
    private CompletableFuture<MessageId> completableFuture;
    private Producer producer;

    @BeforeMethod
    public void setup() throws PulsarClientException {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");
        workerConfig.setSchedulerClassName(RoundRobinScheduler.class.getName());
        workerConfig.setAssignmentWriteMaxRetries(0);

        producer = mock(Producer.class);
        completableFuture = spy(new CompletableFuture<>());
        completableFuture.complete(MessageId.earliest);
        byte[] bytes = any();
        when(producer.sendAsync(bytes)).thenReturn(completableFuture);

        PulsarClient pulsarClient = mock(PulsarClient.class);
        doReturn(producer).when(pulsarClient).createProducer(any(), any());

        schedulerManager = spy(new SchedulerManager(workerConfig, pulsarClient));
        functionRuntimeManager = mock(FunctionRuntimeManager.class);
        functionMetaDataManager = mock(FunctionMetaDataManager.class);
        membershipManager = mock(MembershipManager.class);
        schedulerManager.setFunctionMetaDataManager(functionMetaDataManager);
        schedulerManager.setFunctionRuntimeManager(functionRuntimeManager);
        schedulerManager.setMembershipManager(membershipManager);
    }

    @Test
    public void testSchedule() throws Exception {

        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(Utils.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        //set version
        doReturn(version).when(functionRuntimeManager).getCurrentAssignmentVersion();

        // single node
        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am not leader
        doReturn(false).when(membershipManager).isLeader();
        callSchedule();
        verify(producer, times(0)).sendAsync(any());

        // i am leader
        doReturn(true).when(membershipManager).isLeader();
        callSchedule();
        verify(producer, times(1)).sendAsync(any(byte[].class));
    }

    @Test
    public void testNothingNewToSchedule() throws Exception {

        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(Utils.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        //set version
        doReturn(version).when(functionRuntimeManager).getCurrentAssignmentVersion();

        // single node
        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(producer, Producer.class.getMethod("sendAsync",
                Object.class));
        Assert.assertEquals(invocations.size(), 1);

        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Request.AssignmentsUpdate assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(send);
        log.info("assignmentsUpdate: {}", assignmentsUpdate);
        Assert.assertEquals(
                Request.AssignmentsUpdate.newBuilder().setVersion(version + 1)
                        .addAssignments(assignment1).build(),
                assignmentsUpdate);
    }

    @Test
    public void testAddingFunctions() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(Utils.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        //set version
        doReturn(version).when(functionRuntimeManager).getCurrentAssignmentVersion();

        // single node
        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(producer, Producer.class.getMethod("sendAsync",
                Object.class));
        Assert.assertEquals(invocations.size(), 1);

        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Request.AssignmentsUpdate assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(send);

        log.info("assignmentsUpdate: {}", assignmentsUpdate);
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();
        Assert.assertEquals(
                Request.AssignmentsUpdate.newBuilder().setVersion(version + 1)
                        .addAssignments(assignment1).addAssignments(assignment2).build(),
                assignmentsUpdate);

    }

    @Test
    public void testDeletingFunctions() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        // simulate function2 got removed
        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(Utils.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);
        assignmentEntry1.put(Utils.getFullyQualifiedInstanceId(assignment2.getInstance()), assignment2);

        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        //set version
        doReturn(version).when(functionRuntimeManager).getCurrentAssignmentVersion();

        // single node
        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(producer, Producer.class.getMethod("sendAsync",
                Object.class));
        Assert.assertEquals(invocations.size(), 1);

        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Request.AssignmentsUpdate assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(send);

        log.info("assignmentsUpdate: {}", assignmentsUpdate);

        Assert.assertEquals(
                Request.AssignmentsUpdate.newBuilder().setVersion(version + 1)
                        .addAssignments(assignment1).build(),
                assignmentsUpdate);
    }

    @Test
    public void testScalingUp() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(Utils.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);

        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        //set version
        doReturn(version).when(functionRuntimeManager).getCurrentAssignmentVersion();

        // single node
        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(producer, Producer.class.getMethod("sendAsync",
                Object.class));
        Assert.assertEquals(invocations.size(), 1);

        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Request.AssignmentsUpdate assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(send);

        log.info("assignmentsUpdate: {}", assignmentsUpdate);

        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();
        Assert.assertEquals(
                assignmentsUpdate,
                Request.AssignmentsUpdate.newBuilder().setVersion(version + 1)
                        .addAssignments(assignment1).addAssignments(assignment2).build()
                );

        // scale up

        PulsarClient pulsarClient = mock(PulsarClient.class);
        doReturn(producer).when(pulsarClient).createProducer(any(), any());

        Function.FunctionMetaData function2Scaled = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-2")
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

        invocations = getMethodInvocationDetails(producer, Producer.class.getMethod("sendAsync",
                Object.class));
        Assert.assertEquals(invocations.size(), 2);

        send = (byte[]) invocations.get(1).getRawArguments()[0];
        assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(send);
        Assert.assertEquals(assignmentsUpdate,
                Request.AssignmentsUpdate.newBuilder().setVersion(version + 1 + 1)
                        .addAssignments(assignment1).addAssignments(assignment2Scaled1)
                        .addAssignments(assignment2Scaled2).addAssignments(assignment2Scaled3).build()
                );
    }

    @Test
    public void testScalingDown() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(3)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(Utils.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);

        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        //set version
        doReturn(version).when(functionRuntimeManager).getCurrentAssignmentVersion();

        // single node
        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(producer, Producer.class.getMethod("sendAsync",
                Object.class));
        Assert.assertEquals(invocations.size(), 1);

        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Request.AssignmentsUpdate assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(send);

        log.info("assignmentsUpdate: {}", assignmentsUpdate);

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
        Assert.assertEquals(
                assignmentsUpdate,
                Request.AssignmentsUpdate.newBuilder().setVersion(version + 1)
                        .addAssignments(assignment1).addAssignments(assignment2_1)
                        .addAssignments(assignment2_2).addAssignments(assignment2_3).build()
        );

        // scale down

        PulsarClient pulsarClient = mock(PulsarClient.class);
        doReturn(producer).when(pulsarClient).createProducer(any(), any());

        Function.FunctionMetaData function2Scaled = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-2")
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

        invocations = getMethodInvocationDetails(producer, Producer.class.getMethod("sendAsync",
                Object.class));
        Assert.assertEquals(invocations.size(), 2);

        send = (byte[]) invocations.get(1).getRawArguments()[0];
        assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(send);
        Assert.assertEquals(assignmentsUpdate,
                Request.AssignmentsUpdate.newBuilder().setVersion(version + 1 + 1)
                        .addAssignments(assignment1).addAssignments(assignment2Scaled)
                        .build()
        );
    }

    @Test
    public void testUpdate() throws Exception {
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        long version = 5;
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/foo/bar1"))
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(1)).setVersion(version)
                .build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder()
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/foo/bar1"))
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1").setParallelism(3)).setVersion(version)
                .build();
        functionMetaDataList.add(function1);
        functionMetaDataList.add(function2);
        doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();

        // set assignments
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Map<String, Map<String, Function.Assignment>> currentAssignments = new HashMap<>();
        Map<String, Function.Assignment> assignmentEntry1 = new HashMap<>();
        assignmentEntry1.put(Utils.getFullyQualifiedInstanceId(assignment1.getInstance()), assignment1);

        currentAssignments.put("worker-1", assignmentEntry1);
        doReturn(currentAssignments).when(functionRuntimeManager).getCurrentAssignments();

        //set version
        doReturn(version).when(functionRuntimeManager).getCurrentAssignmentVersion();

        // single node
        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "workerHostname-1", 5000));
        doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        // i am leader
        doReturn(true).when(membershipManager).isLeader();

        callSchedule();

        List<Invocation> invocations = getMethodInvocationDetails(producer, Producer.class.getMethod("sendAsync",
                Object.class));
        Assert.assertEquals(invocations.size(), 1);

        byte[] send = (byte[]) invocations.get(0).getRawArguments()[0];
        Request.AssignmentsUpdate assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(send);

        log.info("assignmentsUpdate: {}", assignmentsUpdate);

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
        Assert.assertEquals(
                assignmentsUpdate,
                Request.AssignmentsUpdate.newBuilder().setVersion(version + 1)
                        .addAssignments(assignment1).addAssignments(assignment2_1)
                        .addAssignments(assignment2_2).addAssignments(assignment2_3).build()
        );

        // scale down

        PulsarClient pulsarClient = mock(PulsarClient.class);
        doReturn(producer).when(pulsarClient).createProducer(any(), any());

        Function.FunctionMetaData function2Updated = Function.FunctionMetaData.newBuilder()
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/foo/bar2"))
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-2")
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

        invocations = getMethodInvocationDetails(producer, Producer.class.getMethod("sendAsync",
                Object.class));
        Assert.assertEquals(invocations.size(), 2);

        send = (byte[]) invocations.get(1).getRawArguments()[0];
        assignmentsUpdate = Request.AssignmentsUpdate.parseFrom(send);
        Assert.assertEquals(assignmentsUpdate,
                Request.AssignmentsUpdate.newBuilder().setVersion(version + 1 + 1)
                        .addAssignments(assignment1).addAssignments(assignment2Updated1)
                        .addAssignments(assignment2Updated2)
                        .addAssignments(assignment2Updated3)
                        .build()
        );
    }

    private void callSchedule() throws NoSuchMethodException, InterruptedException,
            TimeoutException, ExecutionException {
        long intialVersion = functionRuntimeManager.getCurrentAssignmentVersion();
        Future<?> complete = schedulerManager.schedule();

        complete.get(30, TimeUnit.SECONDS);
        doReturn(intialVersion + 1).when(functionRuntimeManager).getCurrentAssignmentVersion();
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
