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
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.worker.WorkerService;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

/**
 * The v2 functions API to manage functions.
 */
public interface FunctionsV2<W extends WorkerService> {

    Response getFunctionInfo(String tenant,
                             String namespace,
                             String functionName,
                             AuthenticationParameters authParams) throws IOException;

    Response getFunctionInstanceStatus(String tenant,
                                               String namespace,
                                               String functionName,
                                               String instanceId,
                                               URI uri,
                                               AuthenticationParameters authParams) throws IOException;

    Response getFunctionStatusV2(String tenant,
                                 String namespace,
                                 String functionName,
                                 URI requestUri,
                                 AuthenticationParameters authParams) throws IOException;

    Response registerFunction(String tenant,
                              String namespace,
                              String functionName,
                              InputStream uploadedInputStream,
                              FormDataContentDisposition fileDetail,
                              String functionPkgUrl,
                              String functionDetailsJson,
                              AuthenticationParameters authParams);


    Response updateFunction(String tenant,
                            String namespace,
                            String functionName,
                            InputStream uploadedInputStream,
                            FormDataContentDisposition fileDetail,
                            String functionPkgUrl,
                            String functionDetailsJson,
                            AuthenticationParameters authParams);

    Response deregisterFunction(String tenant, String namespace, String functionName,
                                AuthenticationParameters authParams);

    Response listFunctions(String tenant, String namespace, AuthenticationParameters authParams);

    Response triggerFunction(String tenant,
                             String namespace,
                             String functionName,
                             String triggerValue,
                             InputStream triggerStream,
                             String topic,
                             AuthenticationParameters authParams);

    Response getFunctionState(String tenant,
                              String namespace,
                              String functionName,
                              String key,
                              AuthenticationParameters authParams);

    Response restartFunctionInstance(String tenant,
                                     String namespace,
                                     String functionName,
                                     String instanceId,
                                     URI uri,
                                     AuthenticationParameters authParams);


    Response restartFunctionInstances(String tenant,
                                      String namespace,
                                      String functionName,
                                      AuthenticationParameters authParams);

    Response stopFunctionInstance(String tenant,
                                  String namespace,
                                  String functionName,
                                  String instanceId,
                                  URI uri,
                                  AuthenticationParameters authParams);

    Response stopFunctionInstances(String tenant,
                                   String namespace,
                                   String functionName,
                                   AuthenticationParameters authParams);

    Response uploadFunction(InputStream uploadedInputStream,
                            String path,
                            AuthenticationParameters authParams);

    Response downloadFunction(String path, AuthenticationParameters authParams);

    List<ConnectorDefinition> getListOfConnectors();

}
