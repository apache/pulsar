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
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.functions.worker.WorkerService;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

/**
 * The v2 functions API to manage functions.
 */
public interface FunctionsV2<W extends WorkerService> {

    Response getFunctionInfo(final String tenant,
                             final String namespace,
                             final String functionName,
                             String clientRole) throws IOException;

    Response getFunctionInstanceStatus(final String tenant,
                                       final String namespace,
                                       final String functionName,
                                       final String instanceId,
                                       URI uri,
                                       String clientRole) throws IOException;

    Response getFunctionStatusV2(String tenant,
                                 String namespace,
                                 String functionName,
                                 URI requestUri,
                                 String clientRole) throws IOException;

    Response registerFunction(String tenant,
                              String namespace,
                              String functionName,
                              InputStream uploadedInputStream,
                              FormDataContentDisposition fileDetail,
                              String functionPkgUrl,
                              String functionDetailsJson,
                              String clientRole);

    Response updateFunction(String tenant,
                            String namespace,
                            String functionName,
                            InputStream uploadedInputStream,
                            FormDataContentDisposition fileDetail,
                            String functionPkgUrl,
                            String functionDetailsJson,
                            String clientRole);

    Response deregisterFunction(String tenant,
                                String namespace,
                                String functionName,
                                String clientAppId);

    Response listFunctions(String tenant, String namespace, String clientRole);

    Response triggerFunction(String tenant,
                             String namespace,
                             String functionName,
                             String triggerValue,
                             InputStream triggerStream,
                             String topic,
                             String clientRole);

    Response getFunctionState(String tenant,
                              String namespace,
                              String functionName,
                              String key,
                              String clientRole);


    Response restartFunctionInstance(String tenant,
                                     String namespace,
                                     String functionName,
                                     String instanceId,
                                     URI uri,
                                     String clientRole);

    Response restartFunctionInstances(String tenant,
                                      String namespace,
                                      String functionName,
                                      String clientRole);

    Response stopFunctionInstance(String tenant,
                                  String namespace,
                                  String functionName,
                                  String instanceId,
                                  URI uri,
                                  String clientRole);

    Response stopFunctionInstances(String tenant,
                                   String namespace,
                                   String functionName,
                                   String clientRole);

    Response uploadFunction(InputStream uploadedInputStream,
                            String path,
                            String clientRole);

    Response downloadFunction(String path, String clientRole);


    List<ConnectorDefinition> getListOfConnectors();

}
