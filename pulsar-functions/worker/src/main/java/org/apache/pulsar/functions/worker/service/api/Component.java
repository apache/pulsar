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
 * @param <W> worker service type
 */
public interface Component<W extends WorkerService> {

    W worker();

    void deregisterFunction(final String tenant,
                            final String namespace,
                            final String componentName,
                            final String clientRole,
                            AuthenticationDataHttps clientAuthenticationDataHttps);

    FunctionConfig getFunctionInfo(final String tenant,
                                   final String namespace,
                                   final String componentName,
                                   final String clientRole,
                                   final AuthenticationDataSource clientAuthenticationDataHttps);

    void stopFunctionInstance(final String tenant,
                              final String namespace,
                              final String componentName,
                              final String instanceId,
                              final URI uri,
                              final String clientRole,
                              final AuthenticationDataSource clientAuthenticationDataHttps);

    void startFunctionInstance(final String tenant,
                               final String namespace,
                               final String componentName,
                               final String instanceId,
                               final URI uri,
                               final String clientRole,
                               final AuthenticationDataSource clientAuthenticationDataHttps);

    void restartFunctionInstance(final String tenant,
                                 final String namespace,
                                 final String componentName,
                                 final String instanceId,
                                 final URI uri,
                                 final String clientRole,
                                 final AuthenticationDataSource clientAuthenticationDataHttps);

    void startFunctionInstances(final String tenant,
                                final String namespace,
                                final String componentName,
                                final String clientRole,
                                final AuthenticationDataSource clientAuthenticationDataHttps);

    void stopFunctionInstances(final String tenant,
                               final String namespace,
                               final String componentName,
                               final String clientRole,
                               final AuthenticationDataSource clientAuthenticationDataHttps);

    void restartFunctionInstances(final String tenant,
                                  final String namespace,
                                  final String componentName,
                                  final String clientRole,
                                  final AuthenticationDataSource clientAuthenticationDataHttps);

    FunctionStatsImpl getFunctionStats(final String tenant,
                                       final String namespace,
                                       final String componentName,
                                       final URI uri,
                                       final String clientRole,
                                       final AuthenticationDataSource clientAuthenticationDataHttps);

    FunctionInstanceStatsDataImpl getFunctionsInstanceStats(final String tenant,
                                                            final String namespace,
                                                            final String componentName,
                                                            final String instanceId,
                                                            final URI uri,
                                                            final String clientRole,
                                                            final AuthenticationDataSource clientAuthenticationDataHttps);

    String triggerFunction(final String tenant,
                           final String namespace,
                           final String functionName,
                           final String input,
                           final InputStream uploadedInputStream,
                           final String topic,
                           final String clientRole,
                           final AuthenticationDataSource clientAuthenticationDataHttps);

    List<String> listFunctions(final String tenant,
                               final String namespace,
                               final String clientRole,
                               final AuthenticationDataSource clientAuthenticationDataHttps);

    FunctionState getFunctionState(final String tenant,
                                   final String namespace,
                                   final String functionName,
                                   final String key,
                                   final String clientRole,
                                   final AuthenticationDataSource clientAuthenticationDataHttps);

    void putFunctionState(final String tenant,
                          final String namespace,
                          final String functionName,
                          final String key,
                          final FunctionState state,
                          final String clientRole,
                          final AuthenticationDataSource clientAuthenticationDataHttps);

    void uploadFunction(final InputStream uploadedInputStream,
                        final String path,
                        String clientRole,
                        final AuthenticationDataSource clientAuthenticationDataHttps);

    StreamingOutput downloadFunction(String path,
                                     String clientRole,
                                     AuthenticationDataHttps clientAuthenticationDataHttps);

    StreamingOutput downloadFunction(String tenant,
                                     String namespace,
                                     String componentName,
                                     String clientRole,
                                     AuthenticationDataHttps clientAuthenticationDataHttps);

    List<ConnectorDefinition> getListOfConnectors();


    void reloadConnectors(String clientRole, AuthenticationDataSource clientAuthenticationDataHttps);
}
