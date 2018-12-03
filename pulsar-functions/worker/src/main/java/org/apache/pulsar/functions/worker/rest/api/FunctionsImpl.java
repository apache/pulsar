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
import org.apache.pulsar.common.policies.data.FunctionStatus;
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
public class FunctionsImpl extends ComponentImpl {

    private class GetFunctionStatus extends GetStatus<FunctionStatus, FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData> {

        @Override
        public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData notScheduledInstance() {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(false);
            functionInstanceStatusData.setError("Function has not been scheduled");
            return functionInstanceStatusData;
        }

        @Override
        public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData fromFunctionStatusProto(
                InstanceCommunication.FunctionStatus status,
                String assignedWorkerId) {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(status.getRunning());
            functionInstanceStatusData.setError(status.getFailureException());
            functionInstanceStatusData.setNumRestarts(status.getNumRestarts());
            functionInstanceStatusData.setNumReceived(status.getNumReceived());
            functionInstanceStatusData.setNumSuccessfullyProcessed(status.getNumSuccessfullyProcessed());
            functionInstanceStatusData.setNumUserExceptions(status.getNumUserExceptions());

            List<ExceptionInformation> userExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                userExceptionInformationList.add(exceptionInformation);
            }
            functionInstanceStatusData.setLatestUserExceptions(userExceptionInformationList);

            functionInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions());
            List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
                ExceptionInformation exceptionInformation
                        = new ExceptionInformation();
                exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
                exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
                systemExceptionInformationList.add(exceptionInformation);
            }
            functionInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

            functionInstanceStatusData.setAverageLatency(status.getAverageLatency());
            functionInstanceStatusData.setLastInvocationTime(status.getLastInvocationTime());
            functionInstanceStatusData.setWorkerId(assignedWorkerId);

            return functionInstanceStatusData;
        }

        @Override
        public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData notRunning(String assignedWorkerId, String error) {
            FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                    = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
            functionInstanceStatusData.setRunning(false);
            if (error != null) {
                functionInstanceStatusData.setError(error);
            }
            functionInstanceStatusData.setWorkerId(assignedWorkerId);

            return functionInstanceStatusData;
        }

        @Override
        public FunctionStatus getStatus(String tenant, String namespace, String name, Collection<Function.Assignment>
                assignments, URI uri) throws PulsarAdminException {
            FunctionStatus functionStatus = new FunctionStatus();
            for (Function.Assignment assignment : assignments) {
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData;
                if (isOwner) {
                    functionInstanceStatusData = getComponentInstanceStatus(tenant, namespace, name, assignment
                            .getInstance().getInstanceId(), null);
                } else {
                    functionInstanceStatusData = worker().getFunctionAdmin().functions().getFunctionStatus(
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getTenant(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getNamespace(),
                            assignment.getInstance().getFunctionMetaData().getFunctionDetails().getName(),
                            assignment.getInstance().getInstanceId());
                }

                FunctionStatus.FunctionInstanceStatus instanceStatus = new FunctionStatus.FunctionInstanceStatus();
                instanceStatus.setInstanceId(assignment.getInstance().getInstanceId());
                instanceStatus.setStatus(functionInstanceStatusData);
                functionStatus.addInstance(instanceStatus);
            }

            functionStatus.setNumInstances(functionStatus.instances.size());
            functionStatus.getInstances().forEach(functionInstanceStatus -> {
                if (functionInstanceStatus.getStatus().isRunning()) {
                    functionStatus.numRunning++;
                }
            });
            return functionStatus;
        }

        @Override
        public FunctionStatus getStatusExternal(String tenant, String namespace, String name, int parallelism) {
            FunctionStatus functionStatus = new FunctionStatus();
            for (int i = 0; i < parallelism; ++i) {
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                        = getComponentInstanceStatus(tenant, namespace, name, i, null);
                FunctionStatus.FunctionInstanceStatus functionInstanceStatus
                        = new FunctionStatus.FunctionInstanceStatus();
                functionInstanceStatus.setInstanceId(i);
                functionInstanceStatus.setStatus(functionInstanceStatusData);
                functionStatus.addInstance(functionInstanceStatus);
            }

            functionStatus.setNumInstances(functionStatus.instances.size());
            functionStatus.getInstances().forEach(functionInstanceStatus -> {
                if (functionInstanceStatus.getStatus().isRunning()) {
                    functionStatus.numRunning++;
                }
            });
            return functionStatus;
        }

        @Override
        public FunctionStatus emptyStatus(int parallelism) {
            FunctionStatus functionStatus = new FunctionStatus();
            functionStatus.setNumInstances(parallelism);
            functionStatus.setNumRunning(0);
            for (int i = 0; i < parallelism; i++) {
                FunctionStatus.FunctionInstanceStatus functionInstanceStatus = new FunctionStatus.FunctionInstanceStatus();
                functionInstanceStatus.setInstanceId(i);
                FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData
                        = new FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData();
                functionInstanceStatusData.setRunning(false);
                functionInstanceStatusData.setError("Function has not been scheduled");
                functionInstanceStatus.setStatus(functionInstanceStatusData);

                functionStatus.addInstance(functionInstanceStatus);
            }

            return functionStatus;
        }
    }

    public FunctionsImpl(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, ComponentType.FUNCTION);
    }

    /**
     * Get status of a function instance.  If this worker is not running the function instance,
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param componentName the function name
     * @param instanceId the function instance id
     * @return the function status
     */
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionInstanceStatus(final String tenant, final String namespace, final String componentName,
                                                                                                      final String instanceId, URI uri) throws IOException {

        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, componentName, Integer.parseInt(instanceId));

        FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData functionInstanceStatusData;
        try {
            functionInstanceStatusData = new GetFunctionStatus().getComponentInstanceStatus(tenant, namespace, componentName,
                    Integer.parseInt(instanceId), uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return functionInstanceStatusData;
    }

    /**
     * Get statuses of all function instances.
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param componentName the function name
     * @return a list of function statuses
     * @throws PulsarAdminException
     */
    public FunctionStatus getFunctionStatus(final String tenant, final String namespace, final String componentName,
                                            URI uri) {

        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName);

        FunctionStatus functionStatus;
        try {
            functionStatus = new GetFunctionStatus().getComponentStatus(tenant, namespace, componentName, uri);
        } catch (WebApplicationException we) {
            throw we;
        } catch (Exception e) {
            log.error("{}/{}/{} Got Exception Getting Status", tenant, namespace, componentName, e);
            throw new RestException(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        return functionStatus;
    }
}