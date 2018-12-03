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
import org.apache.pulsar.common.policies.data.SinkStatus;
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
public class SinkImpl extends ComponentImpl {

    private class GetSinkStatus extends GetStatus<SinkStatus, SinkStatus.SinkInstanceStatus.SinkInstanceStatusData> {

        @Override
        public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData notScheduledInstance() {
            SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                    = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
            sinkInstanceStatusData.setRunning(false);
            sinkInstanceStatusData.setError("Sink has not been scheduled");
            return sinkInstanceStatusData;
        }

        @Override
        public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData fromFunctionStatusProto(
                InstanceCommunication.FunctionStatus status,
                String assignedWorkerId) {
            SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                    = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
            sinkInstanceStatusData.setRunning(status.getRunning());
            sinkInstanceStatusData.setError(status.getFailureException());
            sinkInstanceStatusData.setNumRestarts(status.getNumRestarts());
            sinkInstanceStatusData.setNumReceived(status.getNumReceived());

            List<ExceptionInformation> userExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                userExceptionInformationList.add(exceptionInformation);
            }

            sinkInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions());
            List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                systemExceptionInformationList.add(exceptionInformation);
            }
            sinkInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

            sinkInstanceStatusData.setLastInvocationTime(status.getLastInvocationTime());
            sinkInstanceStatusData.setWorkerId(assignedWorkerId);

            return sinkInstanceStatusData;
        }

        @Override
        public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData notRunning(String assignedWorkerId, String error) {
            SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                    = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
            sinkInstanceStatusData.setRunning(false);
            if (error != null) {
                sinkInstanceStatusData.setError(error);
            }
            sinkInstanceStatusData.setWorkerId(assignedWorkerId);

            return sinkInstanceStatusData;
        }

        @Override
        public SinkStatus getStatus(String tenant, String namespace, String name, Collection<Function.Assignment> assignments, URI uri) throws PulsarAdminException {
            SinkStatus sinkStatus = new SinkStatus();
            for (Function.Assignment assignment : assignments) {
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData;
                if (isOwner) {
                    sinkInstanceStatusData = getComponentInstanceStatus(tenant, namespace, name, assignment.getInstance().getInstanceId(), null);
                } else {
                    sinkInstanceStatusData = worker().getFunctionAdmin().sink().getSinkStatus(
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                            assignment.getInstance().getInstanceId());
                }

                SinkStatus.SinkInstanceStatus instanceStatus = new SinkStatus.SinkInstanceStatus();
                instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
                instanceStatus.setStatus(sinkInstanceStatusData);
                sinkStatus.addInstance(instanceStatus);
            }

            sinkStatus.setNumInstances(sinkStatus.instances.size());
            sinkStatus.getInstances().forEach(sinkInstanceStatus -> {
                if (sinkInstanceStatus.getStatus().isRunning()) {
                    sinkStatus.numRunning++;
                }
            });
            return sinkStatus;
        }

        @Override
        public SinkStatus getStatusExternal (String tenant, String namespace, String name, int parallelism) {
            SinkStatus sinkStatus = new SinkStatus();
            for (int i = 0; i < parallelism; ++i) {
                SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                        = getComponentInstanceStatus(tenant, namespace, name, i, null);
                SinkStatus.SinkInstanceStatus sinkInstanceStatus
                        = new SinkStatus.SinkInstanceStatus();
                sinkInstanceStatus.setInstanceId(i);
                sinkInstanceStatus.setStatus(sinkInstanceStatusData);
                sinkStatus.addInstance(sinkInstanceStatus);
            }

            sinkStatus.setNumInstances(sinkStatus.instances.size());
            sinkStatus.getInstances().forEach(sinkInstanceStatus -> {
                if (sinkInstanceStatus.getStatus().isRunning()) {
                    sinkStatus.numRunning++;
                }
            });
            return sinkStatus;
        }

        @Override
        public SinkStatus emptyStatus(int parallelism) {
            SinkStatus sinkStatus = new SinkStatus();
            sinkStatus.setNumInstances(parallelism);
            sinkStatus.setNumRunning(0);
            for (int i = 0; i < parallelism; i++) {
                SinkStatus.SinkInstanceStatus sinkInstanceStatus = new SinkStatus.SinkInstanceStatus();
                sinkInstanceStatus.setInstanceId(i);
                SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData
                        = new SinkStatus.SinkInstanceStatus.SinkInstanceStatusData();
                sinkInstanceStatusData.setRunning(false);
                sinkInstanceStatusData.setError("Sink has not been scheduled");
                sinkInstanceStatus.setStatus(sinkInstanceStatusData);

                sinkStatus.addInstance(sinkInstanceStatus);
            }

            return sinkStatus;
        }
    }

    public SinkImpl(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, ComponentType.SINK);
    }

    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(
            String tenant, String namespace, String sinkName, String instanceId, URI uri)
            throws IOException {

        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, sinkName, Integer.parseInt(instanceId));


        SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData;
        try {
            sinkInstanceStatusData = new GetSinkStatus().getComponentInstanceStatus(tenant, namespace, sinkName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, sinkName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return sinkInstanceStatusData;
    }

    public SinkStatus getSinkStatus(
            final String tenant, final String namespace,
            final String componentName, URI uri) {

        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName);

        SinkStatus sinkStatus;
        try {
            sinkStatus = new GetSinkStatus().getComponentStatus(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return sinkStatus;
    }
}
