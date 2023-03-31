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
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.authentication.Authentication;
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
                             Authentication authentication) throws IOException;

    @Deprecated
    default Response getFunctionInfo(String tenant,
                             String namespace,
                             String functionName,
                             String clientRole) throws IOException {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return getFunctionInfo(tenant, namespace, functionName, authentication);
    }

    Response getFunctionInstanceStatus(String tenant,
                                               String namespace,
                                               String functionName,
                                               String instanceId,
                                               URI uri,
                                               Authentication authentication) throws IOException;

    @Deprecated
    default Response getFunctionInstanceStatus(String tenant,
                                       String namespace,
                                       String functionName,
                                       String instanceId,
                                       URI uri,
                                       String clientRole) throws IOException {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return getFunctionInstanceStatus(tenant, namespace, functionName, instanceId, uri, authentication);
    }

    Response getFunctionStatusV2(String tenant,
                                 String namespace,
                                 String functionName,
                                 URI requestUri,
                                 Authentication authentication) throws IOException;

    @Deprecated
    default Response getFunctionStatusV2(String tenant,
                                 String namespace,
                                 String functionName,
                                 URI requestUri,
                                 String clientRole) throws IOException {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return getFunctionStatusV2(tenant, namespace, functionName, requestUri, authentication);
    }

    Response registerFunction(String tenant,
                              String namespace,
                              String functionName,
                              InputStream uploadedInputStream,
                              FormDataContentDisposition fileDetail,
                              String functionPkgUrl,
                              String functionDetailsJson,
                              Authentication authentication);

    @Deprecated
    default Response registerFunction(String tenant,
                              String namespace,
                              String functionName,
                              InputStream uploadedInputStream,
                              FormDataContentDisposition fileDetail,
                              String functionPkgUrl,
                              String functionDetailsJson,
                              String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return registerFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail, functionPkgUrl,
                functionDetailsJson, authentication);
    }


    Response updateFunction(String tenant,
                            String namespace,
                            String functionName,
                            InputStream uploadedInputStream,
                            FormDataContentDisposition fileDetail,
                            String functionPkgUrl,
                            String functionDetailsJson,
                            Authentication authentication);

    @Deprecated
    default Response updateFunction(String tenant,
                            String namespace,
                            String functionName,
                            InputStream uploadedInputStream,
                            FormDataContentDisposition fileDetail,
                            String functionPkgUrl,
                            String functionDetailsJson,
                            String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail, functionPkgUrl,
                functionDetailsJson, authentication);
    }

    Response deregisterFunction(String tenant, String namespace, String functionName,
                                Authentication authentication);

    @Deprecated
    default Response deregisterFunction(String tenant,
                                String namespace,
                                String functionName,
                                String clientAppId) {
        Authentication authentication = Authentication.builder().clientRole(clientAppId).build();
        return deregisterFunction(tenant, namespace, functionName, authentication);
    }

    Response listFunctions(String tenant, String namespace, Authentication authentication);

    @Deprecated
    default Response listFunctions(String tenant, String namespace, String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return listFunctions(tenant, namespace, authentication);
    }

    Response triggerFunction(String tenant,
                             String namespace,
                             String functionName,
                             String triggerValue,
                             InputStream triggerStream,
                             String topic,
                             Authentication authentication);

    @Deprecated
    default Response triggerFunction(String tenant,
                             String namespace,
                             String functionName,
                             String triggerValue,
                             InputStream triggerStream,
                             String topic,
                             String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return triggerFunction(tenant, namespace, functionName, triggerValue, triggerStream, topic, authentication);
    }

    Response getFunctionState(String tenant,
                              String namespace,
                              String functionName,
                              String key,
                              Authentication authentication);

    @Deprecated
    default Response getFunctionState(String tenant,
                              String namespace,
                              String functionName,
                              String key,
                              String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return getFunctionState(tenant, namespace, functionName, key, authentication);
    }

    Response restartFunctionInstance(String tenant,
                                     String namespace,
                                     String functionName,
                                     String instanceId,
                                     URI uri,
                                     Authentication authentication);

    @Deprecated
    default Response restartFunctionInstance(String tenant,
                                     String namespace,
                                     String functionName,
                                     String instanceId,
                                     URI uri,
                                     String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return restartFunctionInstance(tenant, namespace, functionName, instanceId, uri, authentication);
    }


    Response restartFunctionInstances(String tenant,
                                      String namespace,
                                      String functionName,
                                      Authentication authentication);
    @Deprecated
    default Response restartFunctionInstances(String tenant,
                                      String namespace,
                                      String functionName,
                                      String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return restartFunctionInstances(tenant, namespace, functionName, authentication);
    }

    Response stopFunctionInstance(String tenant,
                                  String namespace,
                                  String functionName,
                                  String instanceId,
                                  URI uri,
                                  Authentication authentication);

    @Deprecated
    default Response stopFunctionInstance(String tenant,
                                  String namespace,
                                  String functionName,
                                  String instanceId,
                                  URI uri,
                                  String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return stopFunctionInstance(tenant, namespace, functionName, instanceId, uri, authentication);
    }

    Response stopFunctionInstances(String tenant,
                                   String namespace,
                                   String functionName,
                                   Authentication authentication);

    @Deprecated
    default Response stopFunctionInstances(String tenant,
                                   String namespace,
                                   String functionName,
                                   String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return stopFunctionInstances(tenant, namespace, functionName, authentication);
    }

    Response uploadFunction(InputStream uploadedInputStream,
                            String path,
                            Authentication authentication);

    @Deprecated
    default Response uploadFunction(InputStream uploadedInputStream,
                            String path,
                            String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return uploadFunction(uploadedInputStream, path, authentication);
    }

    Response downloadFunction(String path, Authentication authentication);

    @Deprecated
    default Response downloadFunction(String path, String clientRole) {
        Authentication authentication = Authentication.builder().clientRole(clientRole).build();
        return downloadFunction(path, authentication);
    }

    List<ConnectorDefinition> getListOfConnectors();

}
