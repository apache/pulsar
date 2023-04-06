/*
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
package org.apache.pulsar.functions.worker.service.api;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.WorkerFunctionInstanceStats;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.functions.worker.WorkerService;

/**
 * The service to manage worker.
 */
public interface Workers<W extends WorkerService> {

    List<WorkerInfo> getCluster(AuthenticationParameters authParams);

    @Deprecated
    default List<WorkerInfo> getCluster(String clientRole) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole).build();
        return getCluster(authParams);
    }

    WorkerInfo getClusterLeader(AuthenticationParameters authParams);

    @Deprecated
    default WorkerInfo getClusterLeader(String clientRole) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole).build();
        return getClusterLeader(authParams);
    }

    Map<String, Collection<String>> getAssignments(AuthenticationParameters authParams);

    @Deprecated
    default Map<String, Collection<String>> getAssignments(String clientRole) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole).build();
        return getAssignments(authParams);
    }

    List<Metrics> getWorkerMetrics(AuthenticationParameters authParams);

    @Deprecated
    default List<Metrics> getWorkerMetrics(String clientRole) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole).build();
        return getWorkerMetrics(authParams);
    }

    List<WorkerFunctionInstanceStats> getFunctionsMetrics(AuthenticationParameters authParams) throws IOException;

    @Deprecated
    default List<WorkerFunctionInstanceStats> getFunctionsMetrics(String clientRole) throws IOException {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole).build();
        return getFunctionsMetrics(authParams);
    }

    List<ConnectorDefinition> getListOfConnectors(AuthenticationParameters authParams);

    @Deprecated
    default List<ConnectorDefinition> getListOfConnectors(String clientRole) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole).build();
        return getListOfConnectors(authParams);
    }

    void rebalance(URI uri, AuthenticationParameters authParams);

    @Deprecated
    default void rebalance(URI uri, String clientRole) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole).build();
        rebalance(uri, authParams);
    }

    void drain(URI uri, String workerId, AuthenticationParameters authParams, boolean leaderUri);

    @Deprecated
    default void drain(URI uri, String workerId, String clientRole, boolean leaderUri) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole).build();
        drain(uri, workerId, authParams, leaderUri);
    }

    LongRunningProcessStatus getDrainStatus(URI uri, String workerId, AuthenticationParameters authParams,
                                            boolean leaderUri);

    @Deprecated
    default LongRunningProcessStatus getDrainStatus(URI uri, String workerId, String clientRole,
                                            boolean leaderUri) {
        AuthenticationParameters authParams = AuthenticationParameters.builder().clientRole(clientRole).build();
        return getDrainStatus(uri, workerId, authParams, leaderUri);
    }

    boolean isLeaderReady();

    @Deprecated
    default Boolean isLeaderReady(String clientRole) {
        return isLeaderReady();
    }

}
