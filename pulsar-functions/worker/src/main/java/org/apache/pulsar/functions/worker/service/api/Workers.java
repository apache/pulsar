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

    WorkerInfo getClusterLeader(AuthenticationParameters authParams);

    Map<String, Collection<String>> getAssignments(AuthenticationParameters authParams);

    List<Metrics> getWorkerMetrics(AuthenticationParameters authParams);

    List<WorkerFunctionInstanceStats> getFunctionsMetrics(AuthenticationParameters authParams) throws IOException;

    List<ConnectorDefinition> getListOfConnectors(AuthenticationParameters authParams);

    void rebalance(URI uri, AuthenticationParameters authParams);

    void drain(URI uri, String workerId, AuthenticationParameters authParams, boolean leaderUri);

    LongRunningProcessStatus getDrainStatus(URI uri, String workerId, AuthenticationParameters authParams,
                                            boolean leaderUri);

    boolean isLeaderReady(AuthenticationParameters authParams);

}
