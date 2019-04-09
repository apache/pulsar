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
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.ExceptionInformation;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.ComponentType;
import org.apache.pulsar.functions.utils.SinkConfigUtils;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.RestException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
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
            sinkInstanceStatusData.setNumReadFromPulsar(status.getNumReceived());

            // We treat source/user/system exceptions returned from function as system exceptions
            sinkInstanceStatusData.setNumSystemExceptions(status.getNumSystemExceptions()
                    + status.getNumUserExceptions() + status.getNumSourceExceptions());
            List<ExceptionInformation> systemExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestUserExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }

            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSystemExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }

            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSourceExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                systemExceptionInformationList.add(exceptionInformation);
            }
            sinkInstanceStatusData.setLatestSystemExceptions(systemExceptionInformationList);

            sinkInstanceStatusData.setNumSinkExceptions(status.getNumSinkExceptions());
            List<ExceptionInformation> sinkExceptionInformationList = new LinkedList<>();
            for (InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry : status.getLatestSinkExceptionsList()) {
                ExceptionInformation exceptionInformation = getExceptionInformation(exceptionEntry);
                sinkExceptionInformationList.add(exceptionInformation);
            }
            sinkInstanceStatusData.setLatestSinkExceptions(sinkExceptionInformationList);

            sinkInstanceStatusData.setNumWrittenToSink(status.getNumSuccessfullyProcessed());
            sinkInstanceStatusData.setLastReceivedTime(status.getLastInvocationTime());
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
        public SinkStatus getStatus(final String tenant,
                                    final String namespace,
                                    final String name,
                                    final Collection<Function.Assignment> assignments,
                                    final URI uri) throws PulsarAdminException {
            SinkStatus sinkStatus = new SinkStatus();
            for (Function.Assignment assignment : assignments) {
                boolean isOwner = worker().getWorkerConfig().getWorkerId().equals(assignment.getWorkerId());
                SinkStatus.SinkInstanceStatus.SinkInstanceStatusData sinkInstanceStatusData;
                if (isOwner) {
                    sinkInstanceStatusData = getComponentInstanceStatus(tenant,
                            namespace, name, assignment.getInstance().getInstanceId(), null);
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
        public SinkStatus getStatusExternal(final String tenant,
                                            final String namespace,
                                            final String name,
                                            final int parallelism) {
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
        public SinkStatus emptyStatus(final int parallelism) {
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

    private ExceptionInformation getExceptionInformation(InstanceCommunication.FunctionStatus.ExceptionInformation exceptionEntry) {
        ExceptionInformation exceptionInformation
                = new ExceptionInformation();
        exceptionInformation.setTimestampMs(exceptionEntry.getMsSinceEpoch());
        exceptionInformation.setExceptionString(exceptionEntry.getExceptionString());
        return exceptionInformation;
    }

    public SinkImpl(Supplier<WorkerService> workerServiceSupplier) {
        super(workerServiceSupplier, ComponentType.SINK);
    }

    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkInstanceStatus(final String tenant,
                                                                                      final String namespace,
                                                                                      final String sinkName,
                                                                                      final String instanceId,
                                                                                      final URI uri,
                                                                                      final String clientRole,
                                                                                      final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentInstanceStatusRequestValidate(tenant, namespace, sinkName, Integer.parseInt(instanceId), clientRole, clientAuthenticationDataHttps);


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

    public SinkStatus getSinkStatus(final String tenant,
                                    final String namespace,
                                    final String componentName,
                                    final URI uri,
                                    final String clientRole,
                                    final AuthenticationDataSource clientAuthenticationDataHttps) {

        // validate parameters
        componentStatusRequestValidate(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps);

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

    public SinkConfig getSinkInfo(final String tenant,
                                  final String namespace,
                                  final String componentName) {

        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        // validate parameters
        try {
            validateGetFunctionRequestParams(tenant, namespace, componentName, componentType);
        } catch (IllegalArgumentException e) {
            log.error("Invalid get {} request @ /{}/{}/{}", componentType, tenant, namespace, componentName, e);
            throw new RestException(Response.Status.BAD_REQUEST, e.getMessage());
        }

        FunctionMetaDataManager functionMetaDataManager = worker().getFunctionMetaDataManager();
        if (!functionMetaDataManager.containsFunction(tenant, namespace, componentName)) {
            log.error("{} does not exist @ /{}/{}/{}", componentType, tenant, namespace, componentName);
            throw new RestException(Response.Status.NOT_FOUND, String.format(componentType + " %s doesn't exist", componentName));
        }
        Function.FunctionMetaData functionMetaData = functionMetaDataManager.getFunctionMetaData(tenant, namespace, componentName);
        if (!InstanceUtils.calculateSubjectType(functionMetaData.getFunctionDetails()).equals(componentType)) {
            log.error("{}/{}/{} is not a {}", tenant, namespace, componentName, componentType);
            throw new RestException(Response.Status.NOT_FOUND, String.format(componentType + " %s doesn't exist", componentName));
        }
        SinkConfig config = SinkConfigUtils.convertFromDetails(functionMetaData.getFunctionDetails());
        return config;
    }
}
