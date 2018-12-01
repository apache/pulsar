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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

@Slf4j
public class SourceImpl extends ComponentImpl {
    private class GetSourceStatus extends GetStatus<SourceStatus, SourceStatus.SourceInstanceStatus.SourceInstanceStatusData> {

        @Override
        public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData notScheduledInstance() {
            SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                    = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
            sourceInstanceStatusData.setRunning(false);
            sourceInstanceStatusData.setError("Source has not been scheduled");
            return sourceInstanceStatusData;
        }

        @Override
        public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData fromFunctionStatusProto(
                InstanceCommunication.FunctionStatus status,
                String assignedWorkerId) {
            SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                    = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
            sourceInstanceStatusData.setRunning(status.getRunning());
            sourceInstanceStatusData.setError(status.getFailureException());
            sourceInstanceStatusData.setNumRestarts(status.getNumRestarts());
            sourceInstanceStatusData.setNumReceived(status.getNumReceived());

            List<ExceptionInformation> userExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                userExceptionInformationList.add(exceptionInformation);
            }

            sourceInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions());
            List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                systemExceptionInformationList.add(exceptionInformation);
            }
            sourceInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

            sourceInstanceStatusData.setLastInvocationTime(status.getLastInvocationTime());
            sourceInstanceStatusData.setWorkerId(assignedWorkerId);

            return sourceInstanceStatusData;
        }

        @Override
        public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData notRunning(String assignedWorkerId, String error) {
            SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                    = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
            sourceInstanceStatusData.setRunning(false);
            if (error != null) {
                sourceInstanceStatusData.setError(error);
            }
            sourceInstanceStatusData.setWorkerId(assignedWorkerId);

            return sourceInstanceStatusData;
        }

        @Override
        public SourceStatus getStatus(String tenant, String namespace, String name, Collection<Function.Assignment> assignments, URI uri) throws PulsarAdminException {
            SourceStatus sourceStatus = new SourceStatus();
            for (Function.Assignment assignment : assignments) {
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData;
                if (isOwner) {
                    sourceInstanceStatusData = getComponentInstanceStatus(tenant, namespace, name, assignment.getInstance().getInstanceId(), null);
                } else {
                    sourceInstanceStatusData = worker().getFunctionAdmin().source().getSourceStatus(
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                            assignment.getInstance().getInstanceId());
                }

                SourceStatus.SourceInstanceStatus instanceStatus = new SourceStatus.SourceInstanceStatus();
                instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
                instanceStatus.setStatus(sourceInstanceStatusData);
                sourceStatus.addInstance(instanceStatus);
            }

            sourceStatus.setNumInstances(sourceStatus.instances.size());
            sourceStatus.getInstances().forEach(sourceInstanceStatus -> {
                if (sourceInstanceStatus.getStatus().isRunning()) {
                    sourceStatus.numRunning++;
                }
            });
            return sourceStatus;
        }

        @Override
        public SourceStatus getStatusExternal(String tenant, String namespace, String name, int parallelism) {
            SourceStatus sinkStatus = new SourceStatus();
            for (int i = 0; i < parallelism; ++i) {
                SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                        = getComponentInstanceStatus(tenant, namespace, name, i, null);
                SourceStatus.SourceInstanceStatus sourceInstanceStatus
                        = new SourceStatus.SourceInstanceStatus();
                sourceInstanceStatus.setInstanceId(i);
                sourceInstanceStatus.setStatus(sourceInstanceStatusData);
                sinkStatus.addInstance(sourceInstanceStatus);
            }

            sinkStatus.setNumInstances(sinkStatus.instances.size());
            sinkStatus.getInstances().forEach(sourceInstanceStatus -> {
                if (sourceInstanceStatus.getStatus().isRunning()) {
                    sinkStatus.numRunning++;
                }
            });
            return sinkStatus;
        }

        @Override
        public SourceStatus emptyStatus(int parallelism) {
            SourceStatus sourceStatus = new SourceStatus();
            sourceStatus.setNumInstances(parallelism);
            sourceStatus.setNumRunning(0);
            for (int i = 0; i < parallelism; i++) {
                SourceStatus.SourceInstanceStatus sourceInstanceStatus = new SourceStatus.SourceInstanceStatus();
                sourceInstanceStatus.setInstanceId(i);
                SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData
                        = new SourceStatus.SourceInstanceStatus.SourceInstanceStatusData();
                sourceInstanceStatusData.setRunning(false);
                sourceInstanceStatusData.setError("Source has not been scheduled");
                sourceInstanceStatus.setStatus(sourceInstanceStatusData);

                sourceStatus.addInstance(sourceInstanceStatus);
            }

            return sourceStatus;
        }
    }

    public SourceImpl(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, ComponentType.SOURCE);
    }

    public SourceStatus getSourceStatus(final String tenant, final String namespace,
                                        final String componentName, URI uri) throws IOException {
        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName);

        SourceStatus sourceStatus;
        try {
            sourceStatus = new GetSourceStatus().getComponentStatus(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return sourceStatus;
    }

    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceInstanceStatus(
            String tenant, String namespace, String sourceName, String instanceId, URI uri) {
        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, sourceName, Integer.parseInt(instanceId));

        SourceStatus.SourceInstanceStatus.SourceInstanceStatusData sourceInstanceStatusData;
        try {
            sourceInstanceStatusData = new GetSourceStatus().getComponentInstanceStatus(tenant, namespace, sourceName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, sourceName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return sourceInstanceStatusData;
    }
}
