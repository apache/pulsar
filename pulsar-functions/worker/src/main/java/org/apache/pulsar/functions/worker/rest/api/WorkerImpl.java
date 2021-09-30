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
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsImpl;
import org.apache.pulsar.common.policies.data.WorkerFunctionInstanceStats;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.worker.FunctionRuntimeInfo;
import org.apache.pulsar.functions.worker.FunctionRuntimeManager;
import org.apache.pulsar.functions.worker.MembershipManager;
import org.apache.pulsar.functions.worker.PulsarWorkerService;
import org.apache.pulsar.functions.worker.SchedulerManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.functions.worker.service.api.Workers;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;

@Slf4j
public class WorkerImpl implements Workers<PulsarWorkerService> {

    private final Supplier<PulsarWorkerService> workerServiceSupplier;

    public WorkerImpl(Supplier<PulsarWorkerService> workerServiceSupplier) {
        this.workerServiceSupplier = workerServiceSupplier;
    }

    private PulsarWorkerService worker() {
        try {
            return checkNotNull(workerServiceSupplier.get());
        } catch (Throwable t) {
            log.info("Failed to get worker service", t);
            throw t;
        }
    }

    private boolean isWorkerServiceAvailable() {
        WorkerService workerService = workerServiceSupplier.get();
        if (workerService == null) {
            return false;
        }
        if (!workerService.isInitialized()) {
            return false;
        }
        return true;
    }

    @Override
    public List<WorkerInfo> getCluster(String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        }

        List<WorkerInfo> workers = worker().getMembershipManager().getCurrentMembership();
        return workers;
    }

    @Override
    public WorkerInfo getClusterLeader(String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized to get cluster leader", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        }

        MembershipManager membershipManager = worker().getMembershipManager();
        WorkerInfo leader = membershipManager.getLeader();

        if (leader == null) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, "Leader cannot be determined");
        }

        return leader;
    }

    @Override
    public Map<String, Collection<String>> getAssignments(String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized to get cluster assignments", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        Map<String, Map<String, Function.Assignment>> assignments = functionRuntimeManager.getCurrentAssignments();
        Map<String, Collection<String>> ret = new HashMap<>();
        for (Map.Entry<String, Map<String, Function.Assignment>> entry : assignments.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().keySet());
        }
        return ret;
    }

    private boolean isSuperUser(final String clientRole) {
        return clientRole != null && worker().getWorkerConfig().getSuperUserRoles().contains(clientRole);
    }

    @Override
    public List<org.apache.pulsar.common.stats.Metrics> getWorkerMetrics(final String clientRole) {
        if (!isWorkerServiceAvailable() || worker().getMetricsGenerator() == null) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized to get worker stats", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        }
        return worker().getMetricsGenerator().generate();
    }

    @Override
    public List<WorkerFunctionInstanceStats> getFunctionsMetrics(String clientRole) throws IOException {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized to get function stats", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        }

        Map<String, FunctionRuntimeInfo> functionRuntimes = worker().getFunctionRuntimeManager()
                .getFunctionRuntimeInfos();

        List<WorkerFunctionInstanceStats> metricsList = new ArrayList<>(functionRuntimes.size());

        for (Map.Entry<String, FunctionRuntimeInfo> entry : functionRuntimes.entrySet()) {
            String fullyQualifiedInstanceName = entry.getKey();
            FunctionRuntimeInfo functionRuntimeInfo = entry.getValue();

            if (worker().getFunctionRuntimeManager().getRuntimeFactory().externallyManaged()) {
                Function.FunctionDetails functionDetails = functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().getFunctionDetails();
                int parallelism = functionDetails.getParallelism();
                for (int i = 0; i < parallelism; ++i) {
                    FunctionInstanceStatsImpl functionInstanceStats =
                            WorkerUtils.getFunctionInstanceStats(fullyQualifiedInstanceName, functionRuntimeInfo, i);
                    WorkerFunctionInstanceStats workerFunctionInstanceStats = new WorkerFunctionInstanceStats();
                    workerFunctionInstanceStats.setName(FunctionCommon.getFullyQualifiedInstanceId(
                            functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName(), i
                    ));
                    workerFunctionInstanceStats.setMetrics(functionInstanceStats.getMetrics());
                    metricsList.add(workerFunctionInstanceStats);
                }
            } else {
                FunctionInstanceStatsImpl functionInstanceStats =
                        WorkerUtils.getFunctionInstanceStats(fullyQualifiedInstanceName, functionRuntimeInfo,
                                functionRuntimeInfo.getFunctionInstance().getInstanceId());
                WorkerFunctionInstanceStats workerFunctionInstanceStats = new WorkerFunctionInstanceStats();
                workerFunctionInstanceStats.setName(fullyQualifiedInstanceName);
                workerFunctionInstanceStats.setMetrics(functionInstanceStats.getMetrics());
                metricsList.add(workerFunctionInstanceStats);
            }
        }
        return metricsList;
    }

    @Override
    public List<ConnectorDefinition> getListOfConnectors(String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        }

        return this.worker().getConnectorsManager().getConnectorDefinitions();
    }

    public void rebalance(final URI uri, final String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not authorized rebalance cluster", clientRole);
            throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
        }

        if (worker().getLeaderService().isLeader()) {
            try {
                worker().getSchedulerManager().rebalanceIfNotInprogress();
            } catch (SchedulerManager.RebalanceInProgressException e) {
                throw new RestException(Status.BAD_REQUEST, "Rebalance already in progress");
            }
        } else {
            WorkerInfo workerInfo = worker().getMembershipManager().getLeader();
            URI redirect = UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }
    }

    @Override
    public Boolean isLeaderReady(final String clientRole) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }
        if (worker().getLeaderService().isLeader()) {
            return true;
        } else {
            throwUnavailableException();
            return false; // make compiler happy
        }
    }
}
