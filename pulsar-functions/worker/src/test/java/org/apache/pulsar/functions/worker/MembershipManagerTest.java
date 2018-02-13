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

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.fs.MetricsConfig;
import org.apache.pulsar.functions.proto.Function;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MembershipManagerTest {

    @Test
    public void testCheckFailuresNoFailures() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setMetricsConfig(new MetricsConfig().setMetricsSinkClassName(FunctionRuntimeManagerTest.TestSink.class.getName()));
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                mock(PulsarClient.class),
                mock(Namespace.class),
                mock(MembershipManager.class)
        ));        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        MembershipManager membershipManager = spy(new MembershipManager(workerConfig, schedulerManager, mock(PulsarClient.class)));

        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "host-1", 8000));
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-2", "host-2", 8001));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionConfig(
                Function.FunctionConfig.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionConfig(
                Function.FunctionConfig.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        List<Function.FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setFunctionMetaData(function1).build();
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-2")
                .setFunctionMetaData(function2).build();

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);


        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
        Assert.assertEquals(membershipManager.unsignedFunctionDurations.size(), 0);
    }

    @Test
    public void testCheckFailuresSomeFailures() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setMetricsConfig(new MetricsConfig().setMetricsSinkClassName(FunctionRuntimeManagerTest.TestSink.class.getName()));
        workerConfig.setRescheduleTimeoutMs(30000);
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                mock(PulsarClient.class),
                mock(Namespace.class),
                mock(MembershipManager.class)
        ));        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        MembershipManager membershipManager = spy(new MembershipManager(workerConfig, schedulerManager, mock(PulsarClient.class)));

        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "host-1", 8000));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionConfig(
                Function.FunctionConfig.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionConfig(
                Function.FunctionConfig.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        List<Function.FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setFunctionMetaData(function1).build();
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-2")
                .setFunctionMetaData(function2).build();

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
        Assert.assertEquals(membershipManager.unsignedFunctionDurations.size(), 1);
        Assert.assertTrue(membershipManager.unsignedFunctionDurations.get("test-tenant/test-namespace/func-2") != null);

        membershipManager.unsignedFunctionDurations.put("test-tenant/test-namespace/func-2",
                membershipManager.unsignedFunctionDurations.get("test-tenant/test-namespace/func-2") - 30001);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(functionRuntimeManager, times(1)).removeAssignments(
                argThat(new ArgumentMatcher<Collection<Function.Assignment>>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof Collection) {
                    Collection<Function.Assignment> assignments = (Collection) o;

                    if (!assignments.contains(assignment2)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));

        verify(schedulerManager, times(1)).schedule();
    }

    @Test
    public void testCheckFailuresSomeUnassigned() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setMetricsConfig(new MetricsConfig().setMetricsSinkClassName(FunctionRuntimeManagerTest.TestSink.class.getName()));
        workerConfig.setRescheduleTimeoutMs(30000);
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                mock(PulsarClient.class),
                mock(Namespace.class),
                mock(MembershipManager.class)
        ));        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        MembershipManager membershipManager = spy(new MembershipManager(workerConfig, schedulerManager, mock(PulsarClient.class)));

        List<MembershipManager.WorkerInfo> workerInfoList = new LinkedList<>();
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-1", "host-1", 8000));
        workerInfoList.add(MembershipManager.WorkerInfo.of("worker-2", "host-2", 8001));

        Mockito.doReturn(workerInfoList).when(membershipManager).getCurrentMembership();

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionConfig(
                Function.FunctionConfig.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionConfig(
                Function.FunctionConfig.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        List<Function.FunctionMetaData> metaDataList = new LinkedList<>();
        metaDataList.add(function1);
        metaDataList.add(function2);

        Mockito.doReturn(metaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setFunctionMetaData(function1).build();

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(0)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
        Assert.assertEquals(membershipManager.unsignedFunctionDurations.size(), 1);
        Assert.assertTrue(membershipManager.unsignedFunctionDurations.get("test-tenant/test-namespace/func-2") != null);

        membershipManager.unsignedFunctionDurations.put("test-tenant/test-namespace/func-2",
                membershipManager.unsignedFunctionDurations.get("test-tenant/test-namespace/func-2") - 30001);

        membershipManager.checkFailures(functionMetaDataManager, functionRuntimeManager, schedulerManager);

        verify(schedulerManager, times(1)).schedule();
        verify(functionRuntimeManager, times(0)).removeAssignments(any());
    }
}
