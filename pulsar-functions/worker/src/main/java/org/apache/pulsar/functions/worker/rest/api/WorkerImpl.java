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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.worker.*;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.util.*;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class WorkerImpl {

    private final Supplier<WorkerService> workerServiceSupplier;

    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public WorkerImpl(Supplier<WorkerService> workerServiceSupplier) {
        this.workerServiceSupplier = workerServiceSupplier;
    }

    private WorkerService worker() {
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

    public Response getCluster() {
        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }
        List<WorkerInfo> workers = worker().getMembershipManager().getCurrentMembership();
        String jsonString = new Gson().toJson(workers);
        return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonString).build();
    }

    public Response getClusterLeader() {
        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        MembershipManager membershipManager = worker().getMembershipManager();
        WorkerInfo leader = membershipManager.getLeader();

        if (leader == null) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData("Leader cannot be determined")).build();
        }

        String jsonString = new Gson().toJson(leader);
        return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(jsonString).build();
    }

    public Response getAssignments() {

        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        FunctionRuntimeManager functionRuntimeManager = worker().getFunctionRuntimeManager();
        Map<String, Map<String, Function.Assignment>> assignments = functionRuntimeManager.getCurrentAssignments();
        Map<String, Collection<String>> ret = new HashMap<>();
        for (Map.Entry<String, Map<String, Function.Assignment>> entry : assignments.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().keySet());
        }
        return Response.status(Status.OK).type(MediaType.APPLICATION_JSON).entity(new Gson().toJson(ret)).build();
    }

    private Response getUnavailableResponse() {
        return Response.status(Status.SERVICE_UNAVAILABLE).type(MediaType.APPLICATION_JSON)
                .entity(new ErrorData(
                        "Function worker service is not done initializing. " + "Please try again in a little while."))
                .build();
    }

    public boolean isSuperUser(String clientRole) {
        return clientRole != null && worker().getWorkerConfig().getSuperUserRoles().contains(clientRole);
    }

    public List<org.apache.pulsar.common.stats.Metrics> getWorkerMetrics(String clientRole) throws IOException {
        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not admin and authorized to get function-stats", clientRole);
            throw new WebApplicationException(Response.status(Status.UNAUTHORIZED).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData(clientRole + " is not authorize to get metrics")).build());
        }
        return getWorkerMetrics();
    }

    private List<org.apache.pulsar.common.stats.Metrics> getWorkerMetrics() {
        if (!isWorkerServiceAvailable()) {
            throw new WebApplicationException(
                    Response.status(Status.SERVICE_UNAVAILABLE).type(MediaType.APPLICATION_JSON)
                            .entity(new ErrorData("Function worker service is not avaialable")).build());
        }
        return worker().getMetricsGenerator().generate();
    }

    public Response getFunctionsMetrics(String clientRole) throws IOException {
        if (worker().getWorkerConfig().isAuthorizationEnabled() && !isSuperUser(clientRole)) {
            log.error("Client [{}] is not admin and authorized to get function-stats", clientRole);
            return Response.status(Status.UNAUTHORIZED).type(MediaType.APPLICATION_JSON)
                    .entity(new ErrorData("client is not authorize to perform operation")).build();
        }
        return getFunctionsMetrics();
    }

    @Data
    public static class WorkerFunctionInstanceStats {
        /** fully qualified function instance name **/
        public String name;
        public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData metrics;
    }

    private Response getFunctionsMetrics() throws IOException {
        if (!isWorkerServiceAvailable()) {
            return getUnavailableResponse();
        }

        WorkerService workerService = worker();
        Map<String, FunctionRuntimeInfo> functionRuntimes = workerService.getFunctionRuntimeManager()
                .getFunctionRuntimeInfos();

        JsonArray metricsMapList = new JsonArray();

        for (Map.Entry<String, FunctionRuntimeInfo> entry : functionRuntimes.entrySet()) {
            String fullyQualifiedInstanceName = entry.getKey();
            FunctionRuntimeInfo functionRuntimeInfo = entry.getValue();

            FunctionStats.FunctionInstanceStats functionInstanceStats = Utils.getFunctionInstanceStats(fullyQualifiedInstanceName, functionRuntimeInfo);

            WorkerFunctionInstanceStats workerFunctionInstanceStats = new WorkerFunctionInstanceStats();
            workerFunctionInstanceStats.setName(fullyQualifiedInstanceName);
            workerFunctionInstanceStats.setMetrics(functionInstanceStats.getMetrics());

            metricsMapList.add(gson.toJsonTree(workerFunctionInstanceStats));
        }
        String jsonResponse = gson.toJson(metricsMapList);

        return Response.status(Status.OK).entity(jsonResponse).build();
    }
}
