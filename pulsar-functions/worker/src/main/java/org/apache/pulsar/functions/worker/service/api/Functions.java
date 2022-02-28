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
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData;
import org.apache.pulsar.functions.worker.WorkerService;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

/**
 * The service to manage functions.
 */
public interface Functions<W extends WorkerService> extends Component<W> {

    void registerFunction(String tenant,
                          String namespace,
                          String functionName,
                          InputStream uploadedInputStream,
                          FormDataContentDisposition fileDetail,
                          String functionPkgUrl,
                          FunctionConfig functionConfig,
                          String clientRole,
                          AuthenticationDataHttps clientAuthenticationDataHttps);

    void updateFunction(String tenant,
                        String namespace,
                        String functionName,
                        InputStream uploadedInputStream,
                        FormDataContentDisposition fileDetail,
                        String functionPkgUrl,
                        FunctionConfig functionConfig,
                        String clientRole,
                        AuthenticationDataHttps clientAuthenticationDataHttps,
                        UpdateOptionsImpl updateOptions);

    void updateFunctionOnWorkerLeader(String tenant,
                                      String namespace,
                                      String functionName,
                                      InputStream uploadedInputStream,
                                      boolean delete,
                                      URI uri,
                                      String clientRole,
                                      AuthenticationDataSource clientAuthenticationDataHttps);

    FunctionStatus getFunctionStatus(String tenant,
                                     String namespace,
                                     String componentName,
                                     URI uri,
                                     String clientRole,
                                     AuthenticationDataSource clientAuthenticationDataHttps);

    FunctionInstanceStatusData getFunctionInstanceStatus(String tenant,
                                                         String namespace,
                                                         String componentName,
                                                         String instanceId,
                                                         URI uri,
                                                         String clientRole,
                                                         AuthenticationDataSource clientAuthenticationDataHttps);

}
