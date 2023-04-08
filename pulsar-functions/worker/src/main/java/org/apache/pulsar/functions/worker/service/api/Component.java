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

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import javax.ws.rs.core.StreamingOutput;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.functions.worker.WorkerService;

/**
 * Provide service API to access components.
 * @param <W> worker service type
 */
public interface Component<W extends WorkerService> {

    W worker();

    void deregisterFunction(String tenant, String namespace, String componentName, AuthenticationParameters authParams);

    FunctionConfig getFunctionInfo(String tenant, String namespace, String componentName,
                                   AuthenticationParameters authParams);

    void stopFunctionInstance(String tenant, String namespace, String componentName, String instanceId, URI uri,
                              AuthenticationParameters authParams);

    void startFunctionInstance(String tenant, String namespace, String componentName, String instanceId, URI uri,
                               AuthenticationParameters authParams);

    void restartFunctionInstance(String tenant, String namespace, String componentName, String instanceId, URI uri,
                                 AuthenticationParameters authParams);

    void startFunctionInstances(String tenant, String namespace, String componentName,
                                AuthenticationParameters authParams);

    void stopFunctionInstances(String tenant, String namespace, String componentName,
                               AuthenticationParameters authParams);

    void restartFunctionInstances(String tenant, String namespace, String componentName,
                                  AuthenticationParameters authParams);

    FunctionStatsImpl getFunctionStats(String tenant, String namespace, String componentName, URI uri,
                                       AuthenticationParameters authParams);

    FunctionInstanceStatsDataImpl getFunctionsInstanceStats(String tenant, String namespace, String componentName,
                                                            String instanceId, URI uri,
                                                            AuthenticationParameters authParams);

    String triggerFunction(String tenant, String namespace, String functionName, String input,
                           InputStream uploadedInputStream, String topic, AuthenticationParameters authParams);

    List<String> listFunctions(String tenant, String namespace, AuthenticationParameters authParams);

    FunctionState getFunctionState(String tenant, String namespace, String functionName, String key,
                                   AuthenticationParameters authParams);

    void putFunctionState(String tenant, String namespace, String functionName, String key, FunctionState state,
                          AuthenticationParameters authParams);

    void uploadFunction(InputStream uploadedInputStream, String path, AuthenticationParameters authParams);

    StreamingOutput downloadFunction(String path, AuthenticationParameters authParams);

    StreamingOutput downloadFunction(String tenant, String namespace, String componentName,
                                     AuthenticationParameters authParams);

    List<ConnectorDefinition> getListOfConnectors();

    void reloadConnectors(AuthenticationParameters authParams);
}
