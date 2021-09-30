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
import org.apache.pulsar.broker.authentication.AuthenticationDataHttps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.policies.data.SourceStatus.SourceInstanceStatus.SourceInstanceStatusData;
import org.apache.pulsar.functions.worker.WorkerService;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

/**
 * The service to manage sources.
 */
public interface Sources<W extends WorkerService> extends Component<W> {

    void registerSource(final String tenant,
                        final String namespace,
                        final String sourceName,
                        final InputStream uploadedInputStream,
                        final FormDataContentDisposition fileDetail,
                        final String sourcePkgUrl,
                        final SourceConfig sourceConfig,
                        final String clientRole,
                        AuthenticationDataHttps clientAuthenticationDataHttps);

    void updateSource(final String tenant,
                      final String namespace,
                      final String sourceName,
                      final InputStream uploadedInputStream,
                      final FormDataContentDisposition fileDetail,
                      final String sourcePkgUrl,
                      final SourceConfig sourceConfig,
                      final String clientRole,
                      AuthenticationDataHttps clientAuthenticationDataHttps,
                      UpdateOptionsImpl updateOptions);


    SourceStatus getSourceStatus(final String tenant,
                                 final String namespace,
                                 final String componentName,
                                 final URI uri,
                                 final String clientRole,
                                 final AuthenticationDataSource clientAuthenticationDataHttps);


    SourceInstanceStatusData getSourceInstanceStatus(final String tenant,
                                                     final String namespace,
                                                     final String sourceName,
                                                     final String instanceId,
                                                     final URI uri,
                                                     final String clientRole,
                                                     final AuthenticationDataSource clientAuthenticationDataHttps);

    SourceConfig getSourceInfo(final String tenant,
                               final String namespace,
                               final String componentName);

    List<ConnectorDefinition> getSourceList();


    List<ConfigFieldDefinition> getSourceConfigDefinition(String name);
}
