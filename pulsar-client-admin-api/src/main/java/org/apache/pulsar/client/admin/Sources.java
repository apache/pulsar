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
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;

/**
 * Admin interface for Source management.
 */
public interface Sources {
    /**
     * Get the list of sources.
     * <p/>
     * Get the list of all the Pulsar Sources.
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
    List<String> listSources(String tenant, String namespace) throws PulsarAdminException;

    /**
     * Get the list of sources asynchronously.
     * <p/>
     * Get the list of all the Pulsar Sources.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>["f1", "f2", "f3"]</code>
     * </pre>
     */
    CompletableFuture<List<String>> listSourcesAsync(String tenant, String namespace);

    /**
     * Get the configuration for the specified source.
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
     * @param source
     *            Source name
     *
     * @return the source configuration
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to get the configuration of the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    SourceConfig getSource(String tenant, String namespace, String source) throws PulsarAdminException;

    /**
     * Get the configuration for the specified source asynchronously.
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
     * @param source
     *            Source name
     *
     * @return the source configuration
     */
    CompletableFuture<SourceConfig> getSourceAsync(String tenant, String namespace, String source);

    /**
     * Create a new source.
     *
     * @param sourceConfig
     *            the source configuration object
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createSource(SourceConfig sourceConfig, String fileName) throws PulsarAdminException;

    /**
     * Create a new source asynchronously.
     *
     * @param sourceConfig
     *            the source configuration object
     */
    CompletableFuture<Void> createSourceAsync(SourceConfig sourceConfig, String fileName);

    /**
     * Create a new source with package url.
     * <p/>
     * Create a new source by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sourceConfig
     *            the source configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws PulsarAdminException
     */
    void createSourceWithUrl(SourceConfig sourceConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Create a new source with package url asynchronously.
     * <p/>
     * Create a new source by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sourceConfig
     *            the source configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     */
    CompletableFuture<Void> createSourceWithUrlAsync(SourceConfig sourceConfig, String pkgUrl);

    /**
     * Update the configuration for a source.
     * <p/>
     *
     * @param sourceConfig
     *            the source configuration object
     *
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateSource(SourceConfig sourceConfig, String fileName) throws PulsarAdminException;

    /**
     * Update the configuration for a source asynchronously.
     * <p/>
     *
     * @param sourceConfig
     *            the source configuration object
     */
    CompletableFuture<Void> updateSourceAsync(SourceConfig sourceConfig, String fileName);

    /**
     * Update the configuration for a source.
     * <p/>
     *
     * @param sourceConfig
     *            the source configuration object
     * @param updateOptions
     *            options for the update operations
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateSource(SourceConfig sourceConfig, String fileName, UpdateOptions updateOptions)
            throws PulsarAdminException;

    /**
     * Update the configuration for a source asynchronously.
     * <p/>
     *
     * @param sourceConfig
     *            the source configuration object
     * @param updateOptions
     *            options for the update operations
     */
    CompletableFuture<Void> updateSourceAsync(SourceConfig sourceConfig, String fileName,
                                              UpdateOptions updateOptions);

    /**
     * Update the configuration for a source.
     * <p/>
     * Update a source by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sourceConfig
     *            the source configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @throws NotAuthorizedException
     *             You don't have admin permission to create the cluster
     * @throws NotFoundException
     *             Cluster doesn't exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void updateSourceWithUrl(SourceConfig sourceConfig, String pkgUrl) throws PulsarAdminException;

    /**
     * Update the configuration for a source asynchronously.
     * <p/>
     * Update a source by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sourceConfig
     *            the source configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     */
    CompletableFuture<Void> updateSourceWithUrlAsync(SourceConfig sourceConfig, String pkgUrl);

    /**
     * Update the configuration for a source.
     * <p/>
     * Update a source by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sourceConfig
     *            the source configuration object
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
    void updateSourceWithUrl(SourceConfig sourceConfig, String pkgUrl, UpdateOptions updateOptions)
            throws PulsarAdminException;

    /**
     * Update the configuration for a source asynchronously.
     * <p/>
     * Update a source by providing url from which fun-pkg can be downloaded. supported url: http/file
     * eg:
     * File: file:/dir/fileName.jar
     * Http: http://www.repo.com/fileName.jar
     *
     * @param sourceConfig
     *            the source configuration object
     * @param pkgUrl
     *            url from which pkg can be downloaded
     * @param updateOptions
     *            options for the update operations
     */
    CompletableFuture<Void> updateSourceWithUrlAsync(
            SourceConfig sourceConfig, String pkgUrl, UpdateOptions updateOptions);

    /**
     * Delete an existing source.
     * <p/>
     * Delete a source
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
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
    void deleteSource(String tenant, String namespace, String source) throws PulsarAdminException;

    /**
     * Delete an existing source asynchronously.
     * <p/>
     * Delete a source
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     */
    CompletableFuture<Void> deleteSourceAsync(String tenant, String namespace, String source);

    /**
     * Gets the current status of a source.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    SourceStatus getSourceStatus(String tenant, String namespace, String source) throws PulsarAdminException;

    /**
     * Gets the current status of a source asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     */
    CompletableFuture<SourceStatus> getSourceStatusAsync(String tenant, String namespace, String source);

    /**
     * Gets the current status of a source instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     * @param id
     *            Source instance-id
     * @return
     * @throws PulsarAdminException
     */
    SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceStatus(
            String tenant, String namespace, String source, int id)
            throws PulsarAdminException;

    /**
     * Gets the current status of a source instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     * @param id
     *            Source instance-id
     * @return
     */
    CompletableFuture<SourceStatus.SourceInstanceStatus.SourceInstanceStatusData> getSourceStatusAsync(
            String tenant, String namespace, String source, int id);

    /**
     * Restart source instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     * @param instanceId
     *            Source instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartSource(String tenant, String namespace, String source, int instanceId) throws PulsarAdminException;

    /**
     * Restart source instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     * @param instanceId
     *            Source instanceId
     */
    CompletableFuture<Void> restartSourceAsync(String tenant, String namespace, String source, int instanceId);

    /**
     * Restart all source instances.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void restartSource(String tenant, String namespace, String source) throws PulsarAdminException;

    /**
     * Restart all source instances asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     */
    CompletableFuture<Void> restartSourceAsync(String tenant, String namespace, String source);

    /**
     * Stop source instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     * @param instanceId
     *            Source instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopSource(String tenant, String namespace, String source, int instanceId) throws PulsarAdminException;

    /**
     * Stop source instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     * @param instanceId
     *            Source instanceId
     */
    CompletableFuture<Void> stopSourceAsync(String tenant, String namespace, String source, int instanceId);

    /**
     * Stop all source instances.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void stopSource(String tenant, String namespace, String source) throws PulsarAdminException;

    /**
     * Stop all source instances asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     */
    CompletableFuture<Void> stopSourceAsync(String tenant, String namespace, String source);

    /**
     * Start source instance.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     * @param instanceId
     *            Source instanceId
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void startSource(String tenant, String namespace, String source, int instanceId) throws PulsarAdminException;

    /**
     * Start source instance asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     * @param instanceId
     *            Source instanceId
     */
    CompletableFuture<Void> startSourceAsync(String tenant, String namespace, String source, int instanceId);

    /**
     * Start all source instances.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void startSource(String tenant, String namespace, String source) throws PulsarAdminException;

    /**
     * Start all source instances asynchronously.
     *
     * @param tenant
     *            Tenant name
     * @param namespace
     *            Namespace name
     * @param source
     *            Source name
     */
    CompletableFuture<Void> startSourceAsync(String tenant, String namespace, String source);

    /**
     * Fetches a list of supported Pulsar IO sources currently running in cluster mode.
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<ConnectorDefinition> getBuiltInSources() throws PulsarAdminException;

    /**
     * Fetches a list of supported Pulsar IO sources currently running in cluster mode asynchronously.
     */
    CompletableFuture<List<ConnectorDefinition>> getBuiltInSourcesAsync();

    /**
     * Reload the available built-in connectors, include Source and Source.
     *
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void reloadBuiltInSources() throws PulsarAdminException;

    /**
     * Reload the available built-in connectors, include Source and Source asynchronously.
     */
    CompletableFuture<Void> reloadBuiltInSourcesAsync();
}
