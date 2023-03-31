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
import org.apache.pulsar.broker.authentication.Authentication;
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

    void deregisterFunction(String tenant, String namespace, String componentName, Authentication authentication);

    @Deprecated
    default void deregisterFunction(String tenant,
                            String namespace,
                            String componentName,
                            String clientRole,
                            AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authentication = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        deregisterFunction(tenant, namespace, componentName, authentication);
    }

    @Deprecated
    default void deregisterFunction(String tenant,
                            String namespace,
                            String componentName,
                            String clientRole,
                            AuthenticationDataHttps clientAuthenticationDataHttps) {
        Authentication authentication = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        deregisterFunction(tenant, namespace, componentName, authentication);
    }

    FunctionConfig getFunctionInfo(String tenant, String namespace, String componentName,
                                   Authentication authentication);

    @Deprecated
    default FunctionConfig getFunctionInfo(String tenant,
                                   String namespace,
                                   String componentName,
                                   String clientRole,
                                   AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return getFunctionInfo(tenant, namespace, componentName, authData);
    }


    void stopFunctionInstance(String tenant, String namespace, String componentName, String instanceId, URI uri,
                              Authentication authentication);

    @Deprecated
    default void stopFunctionInstance(String tenant,
                              String namespace,
                              String componentName,
                              String instanceId,
                              URI uri,
                              String clientRole,
                              AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        stopFunctionInstance(tenant, namespace, componentName, instanceId, uri, authData);
    }

    void startFunctionInstance(String tenant, String namespace, String componentName, String instanceId, URI uri,
                               Authentication authentication);

    @Deprecated
    default void startFunctionInstance(String tenant,
                               String namespace,
                               String componentName,
                               String instanceId,
                               URI uri,
                               String clientRole,
                               AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        startFunctionInstance(tenant, namespace, componentName, instanceId, uri, authData);
    }

    void restartFunctionInstance(String tenant, String namespace, String componentName, String instanceId, URI uri,
                                 Authentication authentication);

    @Deprecated
    default void restartFunctionInstance(String tenant,
                                 String namespace,
                                 String componentName,
                                 String instanceId,
                                 URI uri,
                                 String clientRole,
                                 AuthenticationDataSource clientAuthenticationDataHttps){
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        restartFunctionInstance(tenant, namespace, componentName, instanceId, uri, authData);
    }

    void startFunctionInstances(String tenant, String namespace, String componentName,
                                Authentication authentication);

    @Deprecated
    default void startFunctionInstances(String tenant,
                                String namespace,
                                String componentName,
                                String clientRole,
                                AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        startFunctionInstances(tenant, namespace, componentName, authData);
    }

    void stopFunctionInstances(String tenant, String namespace, String componentName,
                               Authentication authentication);

    @Deprecated
    default void stopFunctionInstances(String tenant,
                               String namespace,
                               String componentName,
                               String clientRole,
                               AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        stopFunctionInstances(tenant, namespace, componentName, authData);
    }

    void restartFunctionInstances(String tenant, String namespace, String componentName,
                                  Authentication authentication);

    @Deprecated
    default void restartFunctionInstances(String tenant,
                                  String namespace,
                                  String componentName,
                                  String clientRole,
                                  AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        restartFunctionInstances(tenant, namespace, componentName, authData);
    }

    FunctionStatsImpl getFunctionStats(String tenant, String namespace, String componentName, URI uri,
                                       Authentication authentication);

    @Deprecated
    default FunctionStatsImpl getFunctionStats(String tenant,
                                       String namespace,
                                       String componentName,
                                       URI uri,
                                       String clientRole,
                                       AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return getFunctionStats(tenant, namespace, componentName, uri, authData);
    }

    FunctionInstanceStatsDataImpl getFunctionsInstanceStats(String tenant, String namespace, String componentName,
                                                            String instanceId, URI uri,
                                                            Authentication authentication);

    @Deprecated
    default FunctionInstanceStatsDataImpl getFunctionsInstanceStats(String tenant,
                                                            String namespace,
                                                            String componentName,
                                                            String instanceId,
                                                            URI uri,
                                                            String clientRole,
                                                            AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return getFunctionsInstanceStats(tenant, namespace, componentName, instanceId, uri, authData);
    }

    String triggerFunction(String tenant, String namespace, String functionName, String input,
                           InputStream uploadedInputStream, String topic, Authentication authentication);

    @Deprecated
    default String triggerFunction(String tenant,
                           String namespace,
                           String functionName,
                           String input,
                           InputStream uploadedInputStream,
                           String topic,
                           String clientRole,
                           AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return triggerFunction(tenant, namespace, functionName, input, uploadedInputStream, topic, authData);
    }

    List<String> listFunctions(String tenant, String namespace, Authentication authentication);

    @Deprecated
    default List<String> listFunctions(String tenant,
                               String namespace,
                               String clientRole,
                               AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return listFunctions(tenant, namespace, authData);
    }

    FunctionState getFunctionState(String tenant, String namespace, String functionName, String key,
                                   Authentication authentication);

    @Deprecated
    default FunctionState getFunctionState(String tenant,
                                   String namespace,
                                   String functionName,
                                   String key,
                                   String clientRole,
                                   AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return getFunctionState(tenant, namespace, functionName, key, authData);
    }

    void putFunctionState(String tenant, String namespace, String functionName, String key, FunctionState state,
                          Authentication authentication);

    @Deprecated
    default void putFunctionState(String tenant,
                          String namespace,
                          String functionName,
                          String key,
                          FunctionState state,
                          String clientRole,
                          AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        putFunctionState(tenant, namespace, functionName, key, state, authData);
    }

    void uploadFunction(InputStream uploadedInputStream, String path, Authentication authentication);

    @Deprecated
    default void uploadFunction(InputStream uploadedInputStream,
                        String path,
                        String clientRole,
                        AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        uploadFunction(uploadedInputStream, path, authData);
    }

    StreamingOutput downloadFunction(String path, Authentication authentication);

    @Deprecated
    default StreamingOutput downloadFunction(String path,
                                     String clientRole,
                                     AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return downloadFunction(path, authData);
    }

    @Deprecated
    default StreamingOutput downloadFunction(String path,
                                     String clientRole,
                                     AuthenticationDataHttps clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return downloadFunction(path, authData);
    }

    StreamingOutput downloadFunction(String tenant, String namespace, String componentName,
                                     Authentication authentication, boolean transformFunction);

    @Deprecated
    default StreamingOutput downloadFunction(String tenant,
                                     String namespace,
                                     String componentName,
                                     String clientRole,
                                     AuthenticationDataSource clientAuthenticationDataHttps,
                                     boolean transformFunction) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return downloadFunction(tenant, namespace, componentName, authData, transformFunction);
    }

    @Deprecated
    default StreamingOutput downloadFunction(String tenant,
                                     String namespace,
                                     String componentName,
                                     String clientRole,
                                     AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return downloadFunction(tenant, namespace, componentName, authData, false);
    }

    @Deprecated
    default StreamingOutput downloadFunction(String tenant,
                                     String namespace,
                                     String componentName,
                                     String clientRole,
                                     AuthenticationDataHttps clientAuthenticationDataHttps) {
        Authentication authData = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        return downloadFunction(tenant, namespace, componentName, authData, false);
    }

    List<ConnectorDefinition> getListOfConnectors();


    @Deprecated
    default void reloadConnectors(String clientRole, AuthenticationDataSource clientAuthenticationDataHttps) {
        Authentication authentication = Authentication.builder().clientRole(clientRole)
                .clientAuthenticationDataSource(clientAuthenticationDataHttps).build();
        reloadConnectors(authentication);
    }

    void reloadConnectors(Authentication authentication);
}
