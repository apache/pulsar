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
package org.apache.pulsar.functions.worker.rest.api;

import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.functions.worker.WorkerService;

import java.io.IOException;
import java.net.URI;
import java.util.function.Supplier;

public class SinkImpl extends FunctionsImplBase {
    public SinkImpl(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, ComponentType.SINK);
    }


    public SinkStatus getSinkStatus(final String tenant, final String namespace,
                                        final String componentName, URI uri) throws IOException {

        FunctionStatus functionStatus = getFunctionStatus(tenant, namespace, componentName, uri);

        SinkStatus sinkStatus = new SinkStatus();

        sinkStatus.setNumInstances(functionStatus.getNumInstances());
        sinkStatus.setNumRunning(functionStatus.getNumRunning());
        functionStatus.getInstances().forEach(functionInstanceStatus -> {
            SinkStatus.SinkInstanceStatus sinkInstanceStatus = new SinkStatus.SinkInstanceStatus();
            sinkInstanceStatus.setInstanceId(functionInstanceStatus.getInstanceId());
            sinkInstanceStatus.setStatus(fromFunctionInstanceStatus(functionInstanceStatus.getStatus()));
            sinkStatus.addInstance(sinkInstanceStatus);
        });
        return sinkStatus;
    }

    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(String tenant,
                                                                                              String namespace,
                                                                                              String sinkName,
                                                                                              String instanceId,
                                                                                              URI requestUri)
            throws IOException {

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                = getFunctionInstanceStatus(tenant, namespace, sinkName, instanceId, requestUri);

        return fromFunctionInstanceStatus(functionInstanceStatusData);
    }

    private static SinkStatus.SinkInstanceStatus.SinkInstanceStatusData fromFunctionInstanceStatus(
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData) {
        SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();

        sinkInstanceStatusData.setError(functionInstanceStatusData.getError());
        sinkInstanceStatusData.setLastInvocationTime(functionInstanceStatusData.getLastInvocationTime());
        sinkInstanceStatusData.setLatestSystemExceptions(functionInstanceStatusData.getLatestSystemExceptions());
        sinkInstanceStatusData.setNumReceived(functionInstanceStatusData.getNumReceived());
        sinkInstanceStatusData.setNumRestarts(functionInstanceStatusData.getNumRestarts());
        sinkInstanceStatusData.setRunning(functionInstanceStatusData.isRunning());
        sinkInstanceStatusData.setWorkerId(functionInstanceStatusData.getWorkerId());
        return sinkInstanceStatusData;
    }
}
