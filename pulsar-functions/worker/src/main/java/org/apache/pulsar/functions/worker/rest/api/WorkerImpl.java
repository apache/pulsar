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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.functions.worker.rest.RestUtils.throwUnavailableException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
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

import org.apache.pulsar.functions.worker.service.api.Workers;

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
        return workerService.isInitialized();
    }

    @Override
    public List<WorkerInfo> getCluster(AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwIfNotSuperUser(authParams, "get cluster");

        List<WorkerInfo> workers = worker().getMembershipManager().getCurrentMembership();
        return workers;
    }

    @Override
    public WorkerInfo getClusterLeader(AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwIfNotSuperUser(authParams, "get cluster leader");

        MembershipManager membershipManager = worker().getMembershipManager();
        WorkerInfo leader = membershipManager.getLeader();

        if (leader == null) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, "Leader cannot be determined");
        }

        return leader;
    }

    @Override
    public Map<String, Collection<String>> getAssignments(AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwIfNotSuperUser(authParams, "get cluster assignments");

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        Map<String, Map<String, Function.Assignment>> assignments = functionRuntimeManager.getCurrentAssignments();
        Map<String, Collection<String>> ret = new HashMap<>();
        for (Map.Entry<String, Map<String, Function.Assignment>> entry : assignments.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().keySet());
        }
        return ret;
    }

    private void throwIfNotSuperUser(AuthenticationParameters authParams, String action) {
        if (worker().getWorkerConfig().isAuthorizationEnabled()) {
            try {
                if (authParams.getClientRole() == null || !worker().getAuthorizationService().isSuperUser(authParams)
                        .get(worker().getWorkerConfig().getMetadataStoreOperationTimeoutSeconds(), SECONDS)) {
                    log.error("Client with role [{}] and originalPrincipal [{}] is not authorized to {}",
                            authParams.getClientRole(), authParams.getOriginalPrincipal(), action);
                    throw new RestException(Status.UNAUTHORIZED, "Client is not authorized to perform operation");
                }
            } catch (ExecutionException | TimeoutException | InterruptedException e) {
                log.warn("Time-out {} sec while checking the role {} originalPrincipal {} is a super user role ",
                        worker().getWorkerConfig().getMetadataStoreOperationTimeoutSeconds(),
                        authParams.getClientRole(), authParams.getOriginalPrincipal());
                throw new RestException(Status.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        }
    }

    @Override
    public List<org.apache.pulsar.common.stats.Metrics> getWorkerMetrics(final AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable() || worker().getMetricsGenerator() == null) {
            throwUnavailableException();
        }
        throwIfNotSuperUser(authParams, "get worker stats");
        return worker().getMetricsGenerator().generate();
    }

    @Override
    public List<WorkerFunctionInstanceStats> getFunctionsMetrics(AuthenticationParameters authParams)
            throws IOException {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        throwIfNotSuperUser(authParams, "get function stats");

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
    public List<ConnectorDefinition> getListOfConnectors(AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }
        throwIfNotSuperUser(authParams, "get list of connectors");
        return this.worker().getConnectorsManager().getConnectorDefinitions();
    }

    @Override
    public void rebalance(final URI uri, final AuthenticationParameters authParams) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }
        throwIfNotSuperUser(authParams, "rebalance cluster");

        if (worker().getLeaderService().isLeader()) {
            try {
                worker().getSchedulerManager().rebalanceIfNotInprogress();
            } catch (SchedulerManager.RebalanceInProgressException e) {
                throw new RestException(Status.BAD_REQUEST, "Rebalance already in progress");
            } catch (SchedulerManager.TooFewWorkersException e) {
                throw new RestException(Status.BAD_REQUEST, "Too few workers (need at least 2)");
            }
        } else {
            WorkerInfo workerInfo = worker().getMembershipManager().getLeader();

            if (workerInfo == null) {
                throw new RestException(Status.INTERNAL_SERVER_ERROR, "Leader cannot be determined");
            }
            URI redirect =
                    UriBuilder.fromUri(uri).host(workerInfo.getWorkerHostname()).port(workerInfo.getPort()).build();
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }
    }

    @Override
    public void drain(final URI uri, final String inWorkerId, final AuthenticationParameters authParams,
                      boolean calledOnLeaderUri) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        final String actualWorkerId = worker().getWorkerConfig().getWorkerId();
        final String workerId = (inWorkerId == null || inWorkerId.isEmpty()) ? actualWorkerId : inWorkerId;

        if (log.isDebugEnabled()) {
            log.debug("drain called with URI={}, inWorkerId={}, workerId={}, clientRole={}, originalPrincipal={}, "
                            + "calledOnLeaderUri={}, on actual worker-id={}",
                    uri, inWorkerId, workerId, authParams.getClientRole(), authParams.getOriginalPrincipal(),
                    calledOnLeaderUri, actualWorkerId);
        }

        throwIfNotSuperUser(authParams, "drain worker");

        // Depending on which operations we decide to allow, we may add checks here to error/exception if
        //      calledOnLeaderUri is true on a non-leader
        //      calledOnLeaderUri is false on a leader
        // For now, deal with everything.

        if (worker().getLeaderService().isLeader()) {
            try {
                worker().getSchedulerManager().drainIfNotInProgress(workerId);
            } catch (SchedulerManager.DrainInProgressException e) {
                throw new RestException(Status.CONFLICT, "Another drain is in progress");
            } catch (SchedulerManager.TooFewWorkersException e) {
                throw new RestException(Status.BAD_REQUEST, "Too few workers (need at least 2)");
            } catch (SchedulerManager.WorkerNotRemovedAfterPriorDrainException e) {
                String errString = "Worker " + workerId + " was not yet removed after a prior drain op; try later";
                throw new RestException(Status.PRECONDITION_FAILED, errString);
            } catch (SchedulerManager.UnknownWorkerException e) {
                String errString = "Worker " + workerId + " is not among the current workers in the system";
                throw new RestException(Status.BAD_REQUEST, errString);
            }
        } else {
            URI redirect = buildRedirectUriForDrainRelatedOp(uri, workerId);
            log.info("Not leader; redirect URI={}", redirect);
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }
    }

    @Override
    public LongRunningProcessStatus getDrainStatus(final URI uri, final String inWorkerId,
                                                   final AuthenticationParameters authParams,
                                                   boolean calledOnLeaderUri) {
        if (!isWorkerServiceAvailable()) {
            throwUnavailableException();
        }

        final String actualWorkerId = worker().getWorkerConfig().getWorkerId();
        final String workerId = (inWorkerId == null || inWorkerId.isEmpty()) ? actualWorkerId : inWorkerId;

        if (log.isDebugEnabled()) {
            log.debug("getDrainStatus called with uri={}, inWorkerId={}, workerId={}, clientRole={}, "
                            + "originalPrincipal={}, calledOnLeaderUri={}, on actual workerId={}",
                    uri, inWorkerId, workerId, authParams.getClientRole(), authParams.getOriginalPrincipal(),
                    calledOnLeaderUri, actualWorkerId);
        }

        throwIfNotSuperUser(authParams, "get drain status of worker");

        // Depending on which operations we decide to allow, we may add checks here to error/exception if
        //      calledOnLeaderUri is true on a non-leader
        //      calledOnLeaderUri is false on a leader
        // For now, deal with everything.

        if (worker().getLeaderService().isLeader()) {
            return worker().getSchedulerManager().getDrainStatus(workerId);
        } else {
            URI redirect = buildRedirectUriForDrainRelatedOp(uri, workerId);
            log.info("Not leader; redirect URI={}", redirect);
            throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
        }
    }

    @Override
    public boolean isLeaderReady(AuthenticationParameters authParams) {
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

    private URI buildRedirectUriForDrainRelatedOp(final URI uri, String workerId) {
        // The incoming URI could be a leader URI (sent to a non-leader), or a non-leader URI.
        // Leader URI example: “/admin/v2/worker/leader/drain?workerId=<WORKER_ID>”
        // Non-leader URI example: “/admin/v2/worker/drain”
        // Use the leader-URI path in both cases for the redirect to the leader.
        String leaderPath = "admin/v2/worker/leader/drain";
        WorkerInfo workerInfo = worker().getMembershipManager().getLeader();
        if (workerInfo == null) {
            throw new RestException(Status.INTERNAL_SERVER_ERROR, "Leader cannot be determined");
        }
        URI redirect = UriBuilder.fromUri(uri)
                .host(workerInfo.getWorkerHostname())
                .port(workerInfo.getPort())
                .replacePath(leaderPath)
                .replaceQueryParam("workerId", workerId)
                .build();

        return redirect;
    }
}
