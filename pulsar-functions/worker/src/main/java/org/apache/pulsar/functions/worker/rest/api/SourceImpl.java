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
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.functions.worker.WorkerService;

import java.io.IOException;
import java.net.URI;
import java.util.function.Supplier;

public class SourceImpl extends FunctionsImplBase {
    public SourceImpl(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, ComponentType.SOURCE);
    }

    public SourceStatus getSourceStatus(final String tenant, final String namespace,
                                        final String componentName, URI uri) throws IOException {

        FunctionStatus functionStatus = getFunctionStatus(tenant, namespace, componentName, uri);

        SourceStatus sourceStatus = new SourceStatus();

        sourceStatus.setNumInstances(functionStatus.getNumInstances());
        sourceStatus.setNumRunning(functionStatus.getNumRunning());
        functionStatus.getInstances().forEach(functionInstanceStatus -> {
            SourceStatus.SourceInstanceStatus sourceInstanceStatus = new SourceStatus.SourceInstanceStatus();
            sourceInstanceStatus.setInstanceId(functionInstanceStatus.getInstanceId());
            sourceInstanceStatus.setStatus(fromFunctionInstanceStatus(functionInstanceStatus.getStatus()));
            sourceStatus.addInstance(sourceInstanceStatus);
        });
        return sourceStatus;
    }

    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceInstanceStatus(String tenant,
                                                                                              String namespace,
                                                                                              String sourceName,
                                                                                              String instanceId,
                                                                                              URI requestUri)
            throws IOException {

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                = getFunctionInstanceStatus(tenant, namespace, sourceName, instanceId, requestUri);

        return fromFunctionInstanceStatus(functionInstanceStatusData);
    }

    private static SourceStatus.SourceInstanceStatus.SourceInstanceStatusData fromFunctionInstanceStatus(
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData) {
        SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();

        sourceInstanceStatusData.setError(functionInstanceStatusData.getError());
        sourceInstanceStatusData.setLastInvocationTime(functionInstanceStatusData.getLastInvocationTime());
        sourceInstanceStatusData.setLatestSystemExceptions(functionInstanceStatusData.getLatestSystemExceptions());
        sourceInstanceStatusData.setNumReceived(functionInstanceStatusData.getNumReceived());
        sourceInstanceStatusData.setNumRestarts(functionInstanceStatusData.getNumRestarts());
        sourceInstanceStatusData.setRunning(functionInstanceStatusData.isRunning());
        sourceInstanceStatusData.setWorkerId(functionInstanceStatusData.getWorkerId());
        return sourceInstanceStatusData;
    }
}
