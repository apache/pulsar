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

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import javax.ws.rs.core.StreamingOutput;
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.functions.worker.WorkerService;

/**
 * Provide service API to access components.
 *
 * @param <W> worker service type
 */
public interface Component<W extends WorkerService> {

    W worker();

    void deregisterFunction(String tenant,
                            String namespace,
                            String componentName,
                            String clientRole,
                            AuthenticationDataSource clientAuthenticationDataHttps);

    @Deprecated
    default void deregisterFunction(String tenant,
                            String namespace,
                            String componentName,
                            String clientRole,
                            AuthenticationDataHttps clientAuthenticationDataHttps) {
        deregisterFunction(
                tenant,
                namespace,
                componentName,
                clientRole,
                (AuthenticationDataSource) clientAuthenticationDataHttps);
    }

    FunctionConfig getFunctionInfo(String tenant,
                                   String namespace,
                                   String componentName,
                                   String clientRole,
                                   AuthenticationDataSource clientAuthenticationDataHttps);

    void stopFunctionInstance(String tenant,
                              String namespace,
                              String componentName,
                              String instanceId,
                              URI uri,
                              String clientRole,
                              AuthenticationDataSource clientAuthenticationDataHttps);

    void startFunctionInstance(String tenant,
                               String namespace,
                               String componentName,
                               String instanceId,
                               URI uri,
                               String clientRole,
                               AuthenticationDataSource clientAuthenticationDataHttps);

    void restartFunctionInstance(String tenant,
                                 String namespace,
                                 String componentName,
                                 String instanceId,
                                 URI uri,
                                 String clientRole,
                                 AuthenticationDataSource clientAuthenticationDataHttps);

    void startFunctionInstances(String tenant,
                                String namespace,
                                String componentName,
                                String clientRole,
                                AuthenticationDataSource clientAuthenticationDataHttps);

    void stopFunctionInstances(String tenant,
                               String namespace,
                               String componentName,
                               String clientRole,
                               AuthenticationDataSource clientAuthenticationDataHttps);

    void restartFunctionInstances(String tenant,
                                  String namespace,
                                  String componentName,
                                  String clientRole,
                                  AuthenticationDataSource clientAuthenticationDataHttps);

    FunctionStatsImpl getFunctionStats(String tenant,
                                       String namespace,
                                       String componentName,
                                       URI uri,
                                       String clientRole,
                                       AuthenticationDataSource clientAuthenticationDataHttps);

    FunctionInstanceStatsDataImpl getFunctionsInstanceStats(String tenant,
                                                            String namespace,
                                                            String componentName,
                                                            String instanceId,
                                                            URI uri,
                                                            String clientRole,
                                                            AuthenticationDataSource clientAuthenticationDataHttps);

    String triggerFunction(String tenant,
                           String namespace,
                           String functionName,
                           String input,
                           InputStream uploadedInputStream,
                           String topic,
                           String clientRole,
                           AuthenticationDataSource clientAuthenticationDataHttps);

    List<String> listFunctions(String tenant,
                               String namespace,
                               String clientRole,
                               AuthenticationDataSource clientAuthenticationDataHttps);

    FunctionState getFunctionState(String tenant,
                                   String namespace,
                                   String functionName,
                                   String key,
                                   String clientRole,
                                   AuthenticationDataSource clientAuthenticationDataHttps);

    void putFunctionState(String tenant,
                          String namespace,
                          String functionName,
                          String key,
                          FunctionState state,
                          String clientRole,
                          AuthenticationDataSource clientAuthenticationDataHttps);

    void uploadFunction(InputStream uploadedInputStream,
                        String path,
                        String clientRole,
                        AuthenticationDataSource clientAuthenticationDataHttps);

    StreamingOutput downloadFunction(String path,
                                     String clientRole,
                                     AuthenticationDataSource clientAuthenticationDataHttps);

    @Deprecated
    default StreamingOutput downloadFunction(String path,
                                     String clientRole,
                                     AuthenticationDataHttps clientAuthenticationDataHttps) {
        return downloadFunction(path, clientRole, (AuthenticationDataSource) clientAuthenticationDataHttps);
    }

    StreamingOutput downloadFunction(String tenant,
                                     String namespace,
                                     String componentName,
                                     String clientRole,
                                     AuthenticationDataSource clientAuthenticationDataHttps,
                                     boolean transformFunction);

    @Deprecated
    default StreamingOutput downloadFunction(String tenant,
                                     String namespace,
                                     String componentName,
                                     String clientRole,
                                     AuthenticationDataSource clientAuthenticationDataHttps) {
        return downloadFunction(tenant, namespace, componentName, clientRole, clientAuthenticationDataHttps, false);
    }

    @Deprecated
    default StreamingOutput downloadFunction(String tenant,
                                     String namespace,
                                     String componentName,
                                     String clientRole,
                                     AuthenticationDataHttps clientAuthenticationDataHttps) {
        return downloadFunction(
                tenant,
                namespace,
                componentName,
                clientRole,
                (AuthenticationDataSource) clientAuthenticationDataHttps,
                false);
    }

    List<ConnectorDefinition> getListOfConnectors();


    void reloadConnectors(String clientRole, AuthenticationDataSource clientAuthenticationDataHttps);
}
