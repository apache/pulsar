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
package org.apache.pulsar.client.admin;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;

/**
 * Admin interface for Sink management.
 */
public interface Sinks {
    /**
     * Get the list of sinks.
     * <p/>
     * Get the list of all the Pulsar Sinks.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["f1", "f2", "f3"]</code>
     * </pre>
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> listSinks(String tenant, String namespace) throws PulsarAdminException;

    /**
     * Get the list of sinks asynchronously.
     * <p/>
     * Get the list of all the Pulsar Sinks.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["f1", "f2", "f3"]</code>
     * </pre>
     */
    CompletableFuture<List<String>> listSinksAsync(String tenant, String namespace);

    /**
     * Get the configuration for the specified sink.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>{ serviceUrl : "http://my-broker.example.com:8080/" }</code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @return the sink configuration
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to get the configuration of the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    SinkConfig getSink(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Get the configuration for the specified sink asynchronously.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>{ serviceUrl : "http://my-broker.example.com:8080/" }</code>
     * </pre>
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @return the sink configuration
     */
    CompletableFuture<SinkConfig> getSinkAsync(String tenant, String namespace, String sink);

    /**
     * Create a new sink.
     *
     * @param sinkConfig
     *            the sink configuration object
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createSink(SinkConfig sinkConfig, String fileName) throws PulsarAdminException;

    /**
     * Create a new sink asynchronously.
     *
     * @param sinkConfig
     *            the sink configuration object
     */
    CompletableFuture<Void> createSinkAsync(SinkConfig sinkConfig, String fileName);

    /**
     * Create a new sink with package url.
     * <p/>
     * Create a new sink by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws PulsarAdminException
     */
    void createSinkWithUrl(SinkConfig sinkConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Create a new sink with package url asynchronously.
     * <p/>
     * Create a new sink by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     */
    CompletableFuture<Void> createSinkWithUrlAsync(SinkConfig sinkConfig, String pkgUrl);

    /**
     * Update the configuration for a sink.
     * <p/>
     *
     * @param sinkConfig
     *            the sink configuration object
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateSink(SinkConfig sinkConfig, String fileName) throws PulsarAdminException;

    /**
     * Update the configuration for a sink asynchronously.
     * <p/>
     *
     * @param sinkConfig
     *            the sink configuration object
     */
    CompletableFuture<Void> updateSinkAsync(SinkConfig sinkConfig, String fileName);

    /**
     * Update the configuration for a sink.
     * <p/>
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param updateOptions
     *            options for the update operations
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateSink(SinkConfig sinkConfig, String fileName, UpdateOptions updateOptions)
            throws PulsarAdminException;

    /**
     * Update the configuration for a sink asynchronously.
     * <p/>
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param updateOptions
     *            options for the update operations
     */
    CompletableFuture<Void> updateSinkAsync(SinkConfig sinkConfig, String fileName,
                                            UpdateOptions updateOptions);

    /**
     * Update the configuration for a sink.
     * <p/>
     * Update a sink by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateSinkWithUrl(SinkConfig sinkConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Update the configuration for a sink asynchronously.
     * <p/>
     * Update a sink by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     */
    CompletableFuture<Void> updateSinkWithUrlAsync(SinkConfig sinkConfig, String pkgUrl);

    /**
     * Update the configuration for a sink.
     * <p/>
     * Update a sink by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @param updateOptions
     *            options for the update operations
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateSinkWithUrl(SinkConfig sinkConfig, String pkgUrl, UpdateOptions updateOptions)
            throws PulsarAdminException;

    /**
     * Update the configuration for a sink asynchronously.
     * <p/>
     * Update a sink by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sinkConfig
     *            the sink configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @param updateOptions
     *            options for the update operations
     */
    CompletableFuture<Void> updateSinkWithUrlAsync(SinkConfig sinkConfig, String pkgUrl,
                                                   UpdateOptions updateOptions);

    /**
     * Delete an existing sink.
     * <p/>
     * Delete a sink
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission
     * @throws NotFoundException
     *             Cluster does not exist
     * @throws PreconditionFailedException
     *             Cluster is not empty
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteSink(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Delete an existing sink asynchronously.
     * <p/>
     * Delete a sink
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     */
    CompletableFuture<Void> deleteSinkAsync(String tenant, String namespace, String sink);

    /**
     * Gets the current status of a sink.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    SinkStatus getSinkStatus(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Gets the current status of a sink asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     */
    CompletableFuture<SinkStatus> getSinkStatusAsync(String tenant, String namespace, String sink);

    /**
     * Gets the current status of a sink instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     * @param id
     *            Sink instance-id
     * @return
     * @throws PulsarAdminException
     */
    SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkStatus(
            String tenant, String namespace, String sink, int id)
            throws PulsarAdminException;

    /**
     * Gets the current status of a sink instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     * @param id
     *            Sink instance-id
     * @return
     */
    CompletableFuture<SinkStatus.SinkInstanceStatus.SinkInstanceStatusData> getSinkStatusAsync(
            String tenant, String namespace, String sink, int id);

    /**
     * Restart sink instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     * @param instanceId
     *            Sink instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartSink(String tenant, String namespace, String sink, int instanceId) throws PulsarAdminException;

    /**
     * Restart sink instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     * @param instanceId
     *            Sink instanceId
     */
    CompletableFuture<Void> restartSinkAsync(String tenant, String namespace, String sink, int instanceId);

    /**
     * Restart all sink instances.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartSink(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Restart all sink instances asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     */
    CompletableFuture<Void> restartSinkAsync(String tenant, String namespace, String sink);

    /**
     * Stop sink instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     * @param instanceId
     *            Sink instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopSink(String tenant, String namespace, String sink, int instanceId) throws PulsarAdminException;

    /**
     * Stop sink instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     * @param instanceId
     *            Sink instanceId
     */
    CompletableFuture<Void> stopSinkAsync(String tenant, String namespace, String sink, int instanceId);

    /**
     * Stop all sink instances.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopSink(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Stop all sink instances asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     */
    CompletableFuture<Void> stopSinkAsync(String tenant, String namespace, String sink);

    /**
     * Start sink instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     * @param instanceId
     *            Sink instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void startSink(String tenant, String namespace, String sink, int instanceId) throws PulsarAdminException;

    /**
     * Start sink instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     * @param instanceId
     *            Sink instanceId
     */
    CompletableFuture<Void> startSinkAsync(String tenant, String namespace, String sink, int instanceId);

    /**
     * Start all sink instances.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void startSink(String tenant, String namespace, String sink) throws PulsarAdminException;

    /**
     * Start all sink instances asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param sink
     *            Sink name
     */
    CompletableFuture<Void> startSinkAsync(String tenant, String namespace, String sink);

    /**
     * Fetches a list of supported Pulsar IO sinks currently running in cluster mode.
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<ConnectorDefinition> getBuiltInSinks() throws PulsarAdminException;

    /**
     * Fetches a list of supported Pulsar IO sinks currently running in cluster mode asynchronously.
     */
    CompletableFuture<List<ConnectorDefinition>> getBuiltInSinksAsync();

    /**
     * Reload the available built-in connectors, include Source and Sink.
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void reloadBuiltInSinks() throws PulsarAdminException;

    /**
     * Reload the available built-in connectors, include Source and Sink asynchronously.
     */
    CompletableFuture<Void> reloadBuiltInSinksAsync();
}
