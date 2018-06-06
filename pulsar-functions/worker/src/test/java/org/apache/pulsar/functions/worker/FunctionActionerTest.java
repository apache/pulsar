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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.PackageLocationMetaData;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.ThreadRuntimeFactory;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionActioner}.
 */
public class FunctionActionerTest {

    /**
     * Validates FunctionActioner tries to download file from bk.
     * 
     * @throws Exception
     */
    @Test
    public void testStartFunctionWithDLNamespace() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        ThreadRuntimeFactory factory = mock(ThreadRuntimeFactory.class);
        Namespace dlogNamespace = mock(Namespace.class);
        // throw exception when dlogNamespace is accessed by actioner and verify it
        final String exceptionMsg = "dl namespace not-found";
        doThrow(new IllegalArgumentException(exceptionMsg)).when(dlogNamespace).openLog(any());
        LinkedBlockingQueue<FunctionAction> queue = new LinkedBlockingQueue<>();

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace, queue);
        Runtime runtime = mock(Runtime.class);
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setTenant("test-tenant")
                        .setNamespace("test-namespace").setName("func-1"))
                .build();
        Function.Instance instance = Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                .build();
        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();

        // actioner should try to download file from bk-dlogNamespace and fails with exception
        try {
            actioner.startFunction(functionRuntimeInfo);
            fail("should have failed with dlogNamespace open");
        } catch (IllegalArgumentException ie) {
            assertEquals(ie.getMessage(), exceptionMsg);
        }
    }

    @Test
    public void testStartFunctionWithPkgUrl() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        RuntimeFactory factory = mock(RuntimeFactory.class);
        Runtime runtime = mock(Runtime.class);
        doReturn(runtime).when(factory).createContainer(any(), any());
        doNothing().when(runtime).start();
        Namespace dlogNamespace = mock(Namespace.class);
        final String exceptionMsg = "dl namespace not-found";
        doThrow(new IllegalArgumentException(exceptionMsg)).when(dlogNamespace).openLog(any());
        LinkedBlockingQueue<FunctionAction> queue = new LinkedBlockingQueue<>();

        @SuppressWarnings("resource")
        FunctionActioner actioner = new FunctionActioner(workerConfig, factory, dlogNamespace, queue);

        // (1) test with file url. functionActioner should be able to consider file-url and it should be able to call
        // RuntimeSpawner
        String pkgPathLocation = Utils.FILE + ":/user/my-file.jar";
        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setTenant("test-tenant")
                        .setNamespace("test-namespace").setName("func-1"))
                .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath(pkgPathLocation).build())
                .build();
        Function.Instance instance = Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                .build();
        FunctionRuntimeInfo functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();

        actioner.startFunction(functionRuntimeInfo);
        verify(runtime, times(1)).start();

        // (2) test with http-url, downloading file from http should fail with UnknownHostException due to invalid url
        pkgPathLocation = "http://invalid/my-file.jar";
        function1 = Function.FunctionMetaData.newBuilder()
                .setFunctionDetails(Function.FunctionDetails.newBuilder().setTenant("test-tenant")
                        .setNamespace("test-namespace").setName("func-1"))
                .setPackageLocation(PackageLocationMetaData.newBuilder().setPackagePath(pkgPathLocation).build())
                .build();
        instance = Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0).build();
        functionRuntimeInfo = mock(FunctionRuntimeInfo.class);
        doReturn(instance).when(functionRuntimeInfo).getFunctionInstance();

        try {
            actioner.startFunction(functionRuntimeInfo);
            fail("Function-Actioner should have tried to donwload file from http-location");
        } catch (UnknownHostException ue) {
            // ok
        }
    }

}
