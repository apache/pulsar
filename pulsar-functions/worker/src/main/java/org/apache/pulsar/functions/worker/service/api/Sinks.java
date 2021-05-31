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
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.policies.data.SinkStatus.SinkInstanceStatus.SinkInstanceStatusData;
import org.apache.pulsar.functions.worker.WorkerService;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

/**
 * The service to manage sinks.
 */
public interface Sinks<W extends WorkerService> extends Component<W> {

    void registerSink(final String tenant,
                      final String namespace,
                      final String sinkName,
                      final InputStream uploadedInputStream,
                      final FormDataContentDisposition fileDetail,
                      final String sinkPkgUrl,
                      final SinkConfig sinkConfig,
                      final String clientRole,
                      AuthenticationDataHttps clientAuthenticationDataHttps);

    void updateSink(final String tenant,
                    final String namespace,
                    final String sinkName,
                    final InputStream uploadedInputStream,
                    final FormDataContentDisposition fileDetail,
                    final String sinkPkgUrl,
                    final SinkConfig sinkConfig,
                    final String clientRole,
                    AuthenticationDataHttps clientAuthenticationDataHttps,
                    UpdateOptionsImpl updateOptions);

    SinkInstanceStatusData getSinkInstanceStatus(final String tenant,
                                                 final String namespace,
                                                 final String sinkName,
                                                 final String instanceId,
                                                 final URI uri,
                                                 final String clientRole,
                                                 final AuthenticationDataSource clientAuthenticationDataHttps);

    SinkStatus getSinkStatus(final String tenant,
                             final String namespace,
                             final String componentName,
                             final URI uri,
                             final String clientRole,
                             final AuthenticationDataSource clientAuthenticationDataHttps);

    SinkConfig getSinkInfo(final String tenant,
                           final String namespace,
                           final String componentName);

    List<ConnectorDefinition> getSinkList();


    List<ConfigFieldDefinition> getSinkConfigDefinition(String name);

}
